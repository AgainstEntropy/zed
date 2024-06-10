use anyhow::{anyhow, Result};
use collections::HashMap;
use fs::{Fs, RealFs};
use futures::{
    channel::mpsc::{self, UnboundedSender},
    future::LocalBoxFuture,
    Future, FutureExt as _,
};
use gpui::{AsyncAppContext, BackgroundExecutor};
use remote::protocol::{read_message, write_message, MessageId};
use rpc::{
    proto::{
        self, AnyTypedEnvelope, Envelope, EnvelopedMessage as _, Error, PeerId, RequestMessage,
    },
    TypedEnvelope,
};
use smol::{io::AsyncWriteExt, stream::StreamExt, Async};
use std::{
    any::TypeId,
    env, io,
    marker::PhantomData,
    path::Path,
    sync::{Arc, Once},
    time::{Instant, UNIX_EPOCH},
};
use text::LineEnding;

fn main() {
    env::set_var("RUST_BACKTRACE", "1");

    let app = gpui::App::new();
    let background = app.background_executor();

    let (request_tx, mut request_rx) = mpsc::unbounded();
    let (response_tx, mut response_rx) = mpsc::unbounded();

    let mut server = Server::new(background.clone());
    let mut stdin = Async::new(io::stdin()).unwrap();
    let mut stdout = Async::new(io::stdout()).unwrap();

    background
        .spawn(async move {
            let mut output_buffer = Vec::new();
            while let Some(response) = response_rx.next().await {
                write_message(&mut stdout, &mut output_buffer, response).await?;
                stdout.flush().await?;
            }
            anyhow::Ok(())
        })
        .detach();

    background
        .spawn(async move {
            let mut input_buffer = Vec::new();
            let connection_id = PeerId { owner_id: 0, id: 0 };
            loop {
                let message = match read_message(&mut stdin, &mut input_buffer).await {
                    Ok(message) => message,
                    Err(error) => {
                        eprintln!("error reading message: {:?}", error);
                        break;
                    }
                };
                if let Some(envelope) =
                    proto::build_typed_envelope(connection_id, Instant::now(), message)
                {
                    request_tx.unbounded_send(envelope).ok();
                }
            }
        })
        .detach();

    app.headless().run(|cx| {
        cx.spawn(move |cx| async move {
            while let Some(request) = request_rx.next().await {
                let response = Arc::new(ResponseInner {
                    id: MessageId(request.message_id()),
                    tx: response_tx.clone(),
                });
                if let Err(error) = server
                    .handle_message(request, response.clone(), cx.clone())
                    .await
                {
                    response.send_error(error);
                }
            }
        })
        .detach();
    });
}

#[derive(Clone)]
struct Server {
    fs: Arc<RealFs>,
    #[allow(unused)]
    executor: BackgroundExecutor,
    handlers: &'static Handlers,
}

struct Handlers(HashMap<TypeId, MessageHandler>);

static mut HANDLERS: Option<Handlers> = None;
static INIT_HANDLERS: Once = Once::new();

type MessageHandler = Box<
    dyn Send
        + Sync
        + Fn(
            Server,
            Box<dyn AnyTypedEnvelope>,
            Arc<ResponseInner>,
            AsyncAppContext,
        ) -> LocalBoxFuture<'static, Result<()>>,
>;

#[derive(Clone)]
struct Response<T: RequestMessage>(Arc<ResponseInner>, PhantomData<T>);

struct ResponseInner {
    id: MessageId,
    tx: UnboundedSender<Envelope>,
}

impl Server {
    pub fn new(executor: BackgroundExecutor) -> Self {
        let handlers = unsafe {
            INIT_HANDLERS.call_once(|| HANDLERS = Some(Self::init()));
            HANDLERS.as_ref().unwrap()
        };

        Self {
            fs: Arc::new(RealFs::new(Default::default(), None)),
            executor,
            handlers,
        }
    }

    fn init() -> Handlers {
        Handlers(HashMap::default())
            .add(Self::ping)
            .add(Self::write_file)
            .add(Self::stat)
            .add(Self::canonicalize)
            .add(Self::read_link)
            .add(Self::read_dir)
            .add(Self::read_file)
    }

    async fn handle_message(
        &mut self,
        message: Box<dyn AnyTypedEnvelope>,
        response: Arc<ResponseInner>,
        cx: AsyncAppContext,
    ) -> Result<()> {
        if let Some(handler) = self.handlers.0.get(&message.payload_type_id()) {
            let type_name = message.payload_type_name();
            eprintln!("received {type_name}");
            let result = handler(self.clone(), message, response, cx).await;
            eprintln!("responded {type_name}");
            result
        } else {
            response.send_error(anyhow!("unhandled request type"));
            Ok(())
        }
    }

    async fn ping(
        self,
        _: proto::Ping,
        response: Response<proto::Ping>,
        _cx: AsyncAppContext,
    ) -> Result<()> {
        response.send(proto::Ack {});
        Ok(())
    }

    async fn read_file(
        self,
        request: proto::ReadFile,
        response: Response<proto::ReadFile>,
        _cx: AsyncAppContext,
    ) -> Result<()> {
        let content = self.fs.load(Path::new(&request.path)).await?;
        response.send(proto::ReadFileResponse { content });
        Ok(())
    }

    async fn read_link(
        self,
        request: proto::ReadLink,
        response: Response<proto::ReadLink>,
        _cx: AsyncAppContext,
    ) -> Result<()> {
        let content = self.fs.read_link(Path::new(&request.path)).await?;
        response.send(proto::PathResponse {
            path: content.to_string_lossy().to_string(),
        });
        Ok(())
    }

    async fn canonicalize(
        self,
        request: proto::Canonicalize,
        response: Response<proto::Canonicalize>,
        _cx: AsyncAppContext,
    ) -> Result<()> {
        let content = self.fs.canonicalize(Path::new(&request.path)).await?;
        response.send(proto::PathResponse {
            path: content.to_string_lossy().to_string(),
        });
        Ok(())
    }

    async fn read_dir(
        self,
        request: proto::ReadDir,
        response: Response<proto::ReadDir>,
        _cx: AsyncAppContext,
    ) -> Result<()> {
        let mut stream = self.fs.read_dir(Path::new(&request.path)).await?;
        let mut paths = Vec::new();
        while let Some(item) = stream.next().await {
            paths.push(item?.to_string_lossy().to_string());
        }
        response.send(proto::ReadDirResponse { paths });
        Ok(())
    }

    // async fn watch(&self, request: proto::Watch, response: Response) -> Result<()> {
    //     let (mut stream, _) = self
    //         .fs
    //         .watch(
    //             Path::new(&request.path),
    //             Duration::from_millis(request.latency),
    //         )
    //         .await;
    //     self.executor
    //         .spawn(async move {
    //             while let Some(event) = stream.next().await {
    //                 response.send(Payload::Event(proto::Event {
    //                     paths: event
    //                         .into_iter()
    //                         .map(|path| path.to_string_lossy().to_string())
    //                         .collect(),
    //                 }))
    //             }
    //         })
    //         .detach();
    //     Ok(())
    // }

    async fn stat(
        self,
        request: proto::Stat,
        response: Response<proto::Stat>,
        _cx: AsyncAppContext,
    ) -> Result<()> {
        let metadata = self.fs.metadata(Path::new(&request.path)).await?;
        if let Some(metadata) = metadata {
            response.send(proto::StatResponse {
                is_dir: metadata.is_dir,
                is_symlink: metadata.is_symlink,
                mtime: metadata
                    .mtime
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                inode: metadata.inode,
            });
        }
        Ok(())
    }

    async fn write_file(
        self,
        request: proto::WriteFile,
        _: Response<proto::WriteFile>,
        _cx: AsyncAppContext,
    ) -> Result<()> {
        self.fs
            .save(
                Path::new(&request.path),
                &request.content.into(),
                if request.line_ending == proto::write_file::LineEnding::Unix as i32 {
                    LineEnding::Unix
                } else {
                    LineEnding::Windows
                },
            )
            .await
    }
}

impl Handlers {
    fn add<F, Fut, M>(mut self, handler: F) -> Self
    where
        F: 'static + Send + Sync + Fn(Server, M, Response<M>, AsyncAppContext) -> Fut,
        Fut: 'static + Future<Output = Result<()>>,
        M: RequestMessage,
    {
        self.0.insert(
            TypeId::of::<M>(),
            Box::new(move |server, envelope, response, cx| {
                let envelope = *envelope.into_any().downcast::<TypedEnvelope<M>>().unwrap();
                handler(
                    server,
                    envelope.payload,
                    Response::<M>(response, PhantomData),
                    cx,
                )
                .boxed_local()
            }),
        );
        self
    }
}

impl<T: RequestMessage> Response<T> {
    fn send(&self, payload: T::Response) {
        self.0
            .send(payload.into_envelope(0, Some(self.0.id.0), None))
    }

    #[allow(unused)]
    fn send_error(&self, error: anyhow::Error) {
        self.0.send_error(error)
    }
}

impl ResponseInner {
    fn send(&self, envelope: Envelope) {
        self.tx.unbounded_send(envelope).ok();
    }

    fn send_error(&self, error: anyhow::Error) {
        self.send(
            Error {
                code: 0,
                tags: Vec::new(),
                message: error.to_string(),
            }
            .into_envelope(0, Some(self.id.0), None),
        )
    }
}

impl Drop for ResponseInner {
    fn drop(&mut self) {
        self.tx
            .unbounded_send(Envelope {
                original_sender_id: None,
                id: 0,
                payload: None,
                responding_to: Some(self.id.0),
            })
            .ok();
    }
}

use futures::channel::mpsc;
use remote::protocol::{read_message, write_message};
use remote_server::Server;
use rpc::proto::{self, PeerId};
use smol::{io::AsyncWriteExt, stream::StreamExt as _, Async};
use std::{env, io, time::Instant};

fn main() {
    env::set_var("RUST_BACKTRACE", "1");
    env_logger::init();

    let app = gpui::App::new();
    let background = app.background_executor();

    let (incoming_tx, mut incoming_rx) = mpsc::unbounded();
    let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded();

    let mut stdin = Async::new(io::stdin()).unwrap();
    let mut stdout = Async::new(io::stdout()).unwrap();

    background
        .spawn(async move {
            let mut output_buffer = Vec::new();
            while let Some(message) = outgoing_rx.next().await {
                write_message(&mut stdout, &mut output_buffer, message).await?;
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
                        log::warn!("error reading message: {:?}", error);
                        break;
                    }
                };
                if let Some(envelope) =
                    proto::build_typed_envelope(connection_id, Instant::now(), message)
                {
                    incoming_tx.unbounded_send(envelope).ok();
                }
            }
        })
        .detach();

    app.headless().run(|cx| {
        let mut server = Server::new(cx);
        cx.spawn(move |cx| async move {
            while let Some(message) = incoming_rx.next().await {
                server
                    .handle_message(message, outgoing_tx.clone(), cx.clone())
                    .await;
            }
        })
        .detach();
    });
}

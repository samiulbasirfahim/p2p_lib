use actix_web::{self, Error, HttpRequest, HttpResponse, HttpServer, web::Path, web::Payload};
use actix_ws::{self, AggregatedMessage};
use serde::Deserialize;

use crate::socket_channel::{ChannelMessage, join_channel, leave_channel};

pub async fn run_server() {
    println!("Starting up server");
    HttpServer::new(|| return actix_web::App::new().configure(register_routes))
        .bind("0.0.0.0:8888")
        .unwrap()
        .run()
        .await
        .unwrap()
}

fn register_routes(cfg: &mut actix_web::web::ServiceConfig) {
    cfg.route(
        "/test_socket/{room_id}",
        actix_web::web::get().to(test_socket),
    );
}

#[derive(Deserialize)]
struct MessagePayload {
    pub message: String,
}

#[derive(Deserialize)]
#[serde(tag = "type", content = "payload")]
enum SocketPayload {
    Message(MessagePayload),
    Echo(String),
}

async fn test_socket(
    req: HttpRequest,
    body: Payload,
    path: Path<usize>,
) -> Result<HttpResponse, Error> {
    let (res, mut session, stream) = actix_ws::handle(&req, body)?;

    let room_id = path.into_inner();
    let tx = join_channel(room_id);
    let mut rx = tx.subscribe();

    let mut session_clone = session.clone();
    actix_web::rt::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            match msg {
                ChannelMessage::Join(_) => {
                    if session_clone
                        .text("New user joined your room")
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                ChannelMessage::Broadcast(msg) => {
                    if session_clone.text(msg).await.is_err() {
                        return;
                    }
                }
            }
        }
    });

    let mut stream = stream
        .aggregate_continuations()
        .max_continuation_size(1024 * 1024);

    actix_web::rt::spawn(async move {
        if session
            .text("Websocket connection established")
            .await
            .is_err()
        {
            return;
        }
        while let Some(Ok(msg)) = stream.recv().await {
            match msg {
                AggregatedMessage::Text(text) => {
                    match serde_json::from_str::<SocketPayload>(&text) {
                        Ok(SocketPayload::Message(x)) => {
                            if tx
                                .send(ChannelMessage::Broadcast(x.message.to_string()))
                                .is_err()
                            {
                                return;
                            }
                        }
                        Ok(SocketPayload::Echo(y)) => {
                            if session.text(y).await.is_err() {
                                return;
                            };
                        }
                        Err(e) => {
                            eprintln!("{}", e);
                            if session.text("Failed to parse payload").await.is_err() {
                                return;
                            }
                            continue;
                        }
                    };
                }
                AggregatedMessage::Binary(bin) => {
                    println!("Received binary message: {:?}", bin);
                    let _ = session.binary(bin).await;
                }
                AggregatedMessage::Close(reason) => {
                    leave_channel(room_id);
                    println!("Connection closed: {:?}", reason);
                    break;
                }
                _ => {
                    println!("Received other message: {:?}", msg);
                }
            };
        }
    });

    Ok(res)
}

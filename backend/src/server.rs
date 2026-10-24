use actix_web::{self, Error, HttpRequest, HttpResponse, HttpServer, Responder, web::Payload};
use actix_ws::{self, AggregatedMessage};
use future_utils::StreamExt as _;

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
    cfg.route("/test_socket", actix_web::web::get().to(test_socket));
}

async fn test_socket(req: HttpRequest, body: Payload) -> Result<HttpResponse, Error> {
    let (res, mut session, stream) = actix_ws::handle(&req, body)?;

    let mut stream = stream
        .aggregate_continuations()
        .max_continuation_size(1 * 1024 * 1024);

    println!("WebSocket connection established");

    actix_web::rt::spawn(async move {
        let _ = session.text("Websocket connection established").await;
        while let Some(msg) = stream.recv().await {}
    });

    Ok(res)
}

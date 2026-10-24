#[actix_web::main]

async fn main() {
    backend::run().await;
}

mod server;
mod socket_channel;

pub async fn run() -> Result<(), String> {
    server::run_server().await;
    Ok(())
}

// Some(data), None

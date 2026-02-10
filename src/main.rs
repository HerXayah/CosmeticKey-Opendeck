mod cosmetic_key;

use openaction::register_action;
use cosmetic_key::CosmeticKeyAction;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .try_init()?;

    log::info!("Registering CosmeticKey action");
    register_action(CosmeticKeyAction).await;
    
    let args: Vec<String> = std::env::args().collect();
    openaction::run(args).await?;
    Ok(())
}

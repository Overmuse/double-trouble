use anyhow::Result;

mod data_download;
mod settings;
mod trading;
use data_download::download_data;
use settings::{RunMode, Settings};
use std::fs::File;
use tracing::subscriber::set_global_default;
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_log::LogTracer;
use tracing_subscriber::{layer::SubscriberExt, Registry};
use trading::run;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenv::dotenv();
    let formatting_layer = BunyanFormattingLayer::new("kouble-trouble".into(), std::io::stdout);
    let subscriber = Registry::default()
        .with(JsonStorageLayer)
        .with(formatting_layer);
    set_global_default(subscriber).expect("Failed to set subscriber");
    LogTracer::init().expect("Failed to set logger");
    let settings = Settings::new()?;
    match settings.app.run_mode {
        RunMode::Download { out_file } => {
            download_data(&settings.app.tickers, File::create(out_file)?).await?
        }
        RunMode::Run { data_file } => {
            run(settings.app.cash, data_file, settings.kafka).await?;
        }
    }
    Ok(())
}

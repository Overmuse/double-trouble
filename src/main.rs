use anyhow::Result;

mod data_download;
mod settings;
mod trading;
use data_download::download_data;
use settings::{RunMode, Settings};
use std::fs::File;
use tracing::subscriber::set_global_default;
use tracing_log::LogTracer;
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use trading::run;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenv::dotenv();
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    set_global_default(subscriber)?;
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

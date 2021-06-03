use anyhow::Result;

mod data_download;
mod settings;
mod trading;
use data_download::download_data;
use settings::{RunMode, Settings};
use std::fs::File;
use trading::run;

#[tokio::main]
async fn main() -> Result<()> {
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

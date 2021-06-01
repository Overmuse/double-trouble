use anyhow::Result;

//mod asset_selection;
mod data_download;
use data_download::download_data;
//mod trading;

//enum RunMode {
//    AssetSelection,
//    Trading,
//}

#[tokio::main]
async fn main() -> Result<()> {
    let tickers = &["AAPL"];
    let out_file = std::fs::File::create("out.json")?;
    download_data(tickers, out_file).await
}

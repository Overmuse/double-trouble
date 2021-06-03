use anyhow::Result;
use bdays::{calendars::us::USSettlement, HolidayCalendar};
use chrono::prelude::*;
use iex::client::Client as IexClient;
use polygon::rest::Client as PolygonClient;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::fs::File;

mod adjustments;
mod dividends;
mod prices;
mod splits;
pub use adjustments::*;
pub use dividends::*;
pub use prices::*;
pub use splits::*;

pub async fn download_data<T: AsRef<str> + std::fmt::Display>(
    tickers: &[T],
    out_file: File,
) -> Result<()> {
    let iex_client = IexClient::from_env()?;
    let polygon_client = PolygonClient::from_env()?;
    let cal = USSettlement;
    let end_date = cal.advance_bdays(Utc::today().naive_utc(), -1);
    let start_date = cal.advance_bdays(end_date, -100);

    let prices = download_price_data(&polygon_client, tickers, start_date, end_date).await;
    let dividends = download_dividends(&iex_client, tickers).await;
    let splits = download_splits(&iex_client, tickers).await;
    let adjusted = adjust_prices(prices, dividends, splits);
    let formatted: HashMap<String, (Vec<DateTime<Utc>>, Vec<Decimal>)> = adjusted
        .into_iter()
        .map(|v| (v.0, v.1.into_iter().unzip()))
        .collect();
    serde_json::to_writer(out_file, &formatted)?;
    Ok(())
}

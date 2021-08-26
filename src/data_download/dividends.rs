use chrono::NaiveDate;
use iex::{client::Client, dividends::GetDividends, Range};
use rust_decimal::prelude::*;
use std::collections::HashMap;
use tracing::error;

pub type DividendData = HashMap<String, Vec<(NaiveDate, Decimal)>>;

pub async fn download_dividends<T: AsRef<str> + std::fmt::Display>(
    client: &Client<'_>,
    tickers: &[T],
) -> DividendData {
    tracing::debug!("Downloading dividends data");
    let queries = tickers.iter().map(|ticker| GetDividends {
        symbol: ticker.as_ref(),
        range: Range::ThreeMonths,
    });
    client
        .send_all(queries)
        .await
        .into_iter()
        .zip(tickers)
        .map(|(res, ticker)| match res {
            Ok(v) if v.is_empty() => (ticker.to_string(), vec![]),
            Ok(dividends) => {
                let data: Vec<(NaiveDate, Decimal)> = dividends
                    .iter()
                    .map(|div| (div.ex_date, div.amount))
                    .collect();
                (ticker.to_string(), data)
            }
            Err(e) => {
                error!("Failed to download dividends for {}. Error: {}", ticker, e);
                (ticker.to_string(), vec![])
            }
        })
        .collect()
}

use chrono::NaiveDate;
use iex::{client::Client, splits::GetSplits, Range};
use rust_decimal::prelude::*;
use std::collections::HashMap;
use tracing::error;

pub type SplitData = HashMap<String, Vec<(NaiveDate, Decimal)>>;

pub async fn download_splits<T: AsRef<str> + std::fmt::Display>(
    client: &Client<'_>,
    tickers: &[T],
) -> SplitData {
    let queries = tickers.iter().map(|ticker| GetSplits {
        symbol: ticker.as_ref(),
        range: Range::ThreeMonths,
    });
    tracing::debug!("Downloading splits data");
    client
        .send_all(queries)
        .await
        .into_iter()
        .zip(tickers)
        .map(|(res, ticker)| match res {
            Ok(v) if v.is_empty() => (ticker.to_string(), vec![]),
            Ok(splits) => {
                let data: Vec<(NaiveDate, Decimal)> = splits
                    .iter()
                    .map(|split| (split.ex_date, Decimal::from_f64(split.ratio).unwrap()))
                    .collect();
                (ticker.to_string(), data)
            }
            Err(e) => {
                error!("Failed to download splits for {}. Error: {}", ticker, e);
                (ticker.to_string(), vec![])
            }
        })
        .collect()
}

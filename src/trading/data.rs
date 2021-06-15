use anyhow::Result;
use csv::Reader;
use polygon::rest::{Client, GetTickerSnapshot};
use rust_decimal::prelude::*;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use tracing::debug;

#[derive(Debug, Deserialize)]
pub struct TradePair {
    pub asset_1: String,
    pub asset_2: String,
    pub original_lt_spread: Decimal,
    pub original_st_spread: Decimal,
    pub epsilon: Decimal,
}

pub fn read_data<T: AsRef<Path>>(file: T) -> Result<Vec<TradePair>> {
    let mut reader = Reader::from_path(file)?;
    Ok(reader.deserialize().filter_map(|x| x.ok()).collect())
}

#[tracing::instrument(skip(client, tickers))]
pub async fn open_close<'a, T: Iterator<Item = &'a str> + 'a>(
    client: &Client<'_>,
    tickers: T,
) -> HashMap<String, (Decimal, Decimal)> {
    debug!("Downloading overnight returns data");
    let reqs = tickers.map(|ticker| GetTickerSnapshot(ticker));
    let results = client.send_all(reqs).await;
    results
        .into_iter()
        .flat_map(|res| res.ok())
        .map(|snapshot| {
            (
                snapshot.ticker.ticker,
                (snapshot.ticker.day.o, snapshot.ticker.previous_day.c),
            )
        })
        .collect()
}

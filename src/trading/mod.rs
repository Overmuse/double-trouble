use anyhow::Result;
use domain::TradeBands;
use kafka_settings::{consumer, producer, KafkaSettings};
use polygon::rest::Client;
use rust_decimal::Decimal;
use std::collections::HashSet;
use std::iter::once;
use std::path::Path;
use tokio::sync::mpsc::unbounded_channel;

mod data;
mod domain;
mod relay;
mod trade_generator;
use relay::Relay;
use trade_generator::TradeGenerator;

pub async fn run<T: AsRef<Path>>(cash: Decimal, data_file: T, kafka: KafkaSettings) -> Result<()> {
    let client = Client::from_env()?;
    let producer = producer(&kafka)?;
    let consumer = consumer(&kafka)?;
    let trade_pairs = data::read_data(data_file)?;
    let tickers: HashSet<String> = trade_pairs
        .iter()
        .flat_map(|pair| once(pair.asset_1.clone()).chain(once(pair.asset_2.clone())))
        .collect();
    let overnight_returns =
        data::overnight_returns(&client, tickers.iter().map(|s| s.as_ref())).await;
    let pairs: Vec<TradeBands> = trade_pairs
        .into_iter()
        .filter_map(|pair| {
            let ret_1 = overnight_returns.get(&pair.asset_1);
            let ret_2 = overnight_returns.get(&pair.asset_2);
            ret_1.zip(ret_2).map(|(r1, r2)| {
                let overnight_spread_change = r1 - r2;
                TradeBands::new(pair, overnight_spread_change)
            })
        })
        .collect();

    let (tx, rx) = unbounded_channel();
    let relay = Relay::new(tickers, consumer, tx);
    let mut trade_generator = TradeGenerator::new(cash, pairs, rx, producer);

    tokio::join!(trade_generator.run(), relay.run());
    Ok(())
}

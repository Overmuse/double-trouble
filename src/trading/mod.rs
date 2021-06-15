use anyhow::Result;
use domain::TradeBands;
use kafka_settings::{consumer, producer, KafkaSettings};
use polygon::rest::Client;
use rust_decimal::prelude::*;
use std::collections::HashSet;
use std::iter::once;
use std::path::Path;
use tokio::sync::mpsc::unbounded_channel;
use tracing::{debug, info};

mod data;
mod domain;
mod relay;
mod trade_generator;
use relay::Relay;
use trade_generator::TradeGenerator;

pub async fn run<T: AsRef<Path>>(cash: Decimal, data_file: T, kafka: KafkaSettings) -> Result<()> {
    info!("Starting double-trouble");
    let client = Client::from_env()?;
    let producer = producer(&kafka)?;
    let consumer = consumer(&kafka)?;
    let trade_pairs = data::read_data(data_file)?;
    let tickers: HashSet<String> = trade_pairs
        .iter()
        .flat_map(|pair| once(pair.asset_1.clone()).chain(once(pair.asset_2.clone())))
        .collect();
    let open_close = data::open_close(&client, tickers.iter().map(|s| s.as_ref())).await;
    let pairs: Vec<TradeBands> = trade_pairs
        .into_iter()
        .filter_map(|pair| {
            let opt1 = open_close.get(&pair.asset_1);
            let opt2 = open_close.get(&pair.asset_2);
            opt1.zip(opt2).map(|((op1, cl1), (op2, cl2))| {
                let equilibrium = (((op1.ln() - op2.ln()) - pair.original_lt_spread)
                    + ((cl1.ln() - cl2.ln()) - pair.original_lt_spread))
                    / Decimal::new(2, 0);
                TradeBands::new(pair, equilibrium)
            })
        })
        .inspect(|pair| debug!("Pair: {:?}", pair))
        .collect();

    let (tx, rx) = unbounded_channel();
    let relay = Relay::new(tickers, consumer, tx);
    let mut trade_generator = TradeGenerator::new(cash, pairs, rx, producer);

    tokio::join!(trade_generator.run(), relay.run());
    Ok(())
}

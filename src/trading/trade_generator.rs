use crate::trading::domain::Position;
use crate::trading::TradeBands;
use polygon::ws::Aggregate;
use position_intents::{AmountSpec, PositionIntent};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rust_decimal::prelude::*;
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{interval_at, Duration, Instant, Interval};
use tracing::error;

pub struct TradeGenerator {
    cash: Decimal,
    pairs: Vec<TradeBands>,
    prices: HashMap<String, Decimal>,
    receiver: UnboundedReceiver<Aggregate>,
    producer: FutureProducer,
    interval: Interval,
}

impl TradeGenerator {
    pub fn new(
        cash: Decimal,
        pairs: Vec<TradeBands>,
        receiver: UnboundedReceiver<Aggregate>,
        producer: FutureProducer,
    ) -> Self {
        let prices = HashMap::new();
        let interval = interval_at(
            // First tick will happen one minute from now...
            Instant::now() + Duration::from_secs(60),
            // ...and then every 5 minutes thereafter
            Duration::from_secs(60 * 5),
        );
        Self {
            cash,
            pairs,
            prices,
            receiver,
            producer,
            interval,
        }
    }

    fn update_price(&mut self, agg: Aggregate) {
        self.prices.insert(agg.symbol, agg.close);
    }

    fn generate_positions(&self) -> Vec<PositionIntent> {
        let mut intents = Vec::new();
        for pair in self.pairs.iter() {
            let p1 = self.prices.get(&pair.asset_1);
            let p2 = self.prices.get(&pair.asset_2);
            let pair_string = format!("{}-{}", pair.asset_1.clone(), pair.asset_2.clone());
            if let Some((p1, p2)) = p1.zip(p2) {
                let position = pair.trade_signal(p1, p2);
                match position {
                    Position::Long => {
                        intents.push(
                            PositionIntent::builder(
                                "double-trouble".to_string(),
                                pair.asset_1.clone(),
                                AmountSpec::Dollars(self.cash / Decimal::new(3, 0)),
                            )
                            .limit_price(p1 * Decimal::new(995, 3))
                            .sub_strategy(pair_string.clone())
                            .build(),
                        );
                        intents.push(
                            PositionIntent::builder(
                                "double-trouble".to_string(),
                                pair.asset_2.clone(),
                                AmountSpec::Dollars(-self.cash / Decimal::new(3, 0)),
                            )
                            .limit_price(p2 * Decimal::new(1005, 3))
                            .sub_strategy(pair_string.clone())
                            .build(),
                        );
                    }
                    Position::Short => {
                        intents.push(
                            PositionIntent::builder(
                                "double-trouble".to_string(),
                                pair.asset_1.clone(),
                                AmountSpec::Dollars(-self.cash / Decimal::new(3, 0)),
                            )
                            .limit_price(p1 * Decimal::new(1005, 3))
                            .sub_strategy(pair_string.clone())
                            .build(),
                        );
                        intents.push(
                            PositionIntent::builder(
                                "double-trouble".to_string(),
                                pair.asset_2.clone(),
                                AmountSpec::Dollars(self.cash / Decimal::new(3, 0)),
                            )
                            .limit_price(p2 * Decimal::new(995, 3))
                            .sub_strategy(pair_string.clone())
                            .build(),
                        );
                    }
                    Position::RetainLong => {
                        intents.push(
                            PositionIntent::builder(
                                "double-trouble".to_string(),
                                pair.asset_1.clone(),
                                AmountSpec::RetainLong,
                            )
                            .sub_strategy(pair_string.clone())
                            .build(),
                        );
                        intents.push(
                            PositionIntent::builder(
                                "double-trouble".to_string(),
                                pair.asset_2.clone(),
                                AmountSpec::RetainShort,
                            )
                            .sub_strategy(pair_string.clone())
                            .build(),
                        );
                    }
                    Position::RetainShort => {
                        intents.push(
                            PositionIntent::builder(
                                "double-trouble".to_string(),
                                pair.asset_1.clone(),
                                AmountSpec::RetainShort,
                            )
                            .sub_strategy(pair_string.clone())
                            .build(),
                        );
                        intents.push(
                            PositionIntent::builder(
                                "double-trouble".to_string(),
                                pair.asset_2.clone(),
                                AmountSpec::RetainLong,
                            )
                            .sub_strategy(pair_string.clone())
                            .build(),
                        );
                    }
                }
            }
        }
        intents
    }

    async fn send_intents(&self, intents: Vec<PositionIntent>) {
        for intent in intents {
            let payload = serde_json::to_vec(&intent).unwrap();
            let record = FutureRecord::to("position-intents")
                .key(&intent.ticker)
                .payload(&payload);
            match self.producer.send_result(record) {
                Ok(fut) => {
                    if let Err((e, _)) = fut.await.unwrap() {
                        error!("Failed to send message.\n {:?}", e)
                    }
                }
                Err((e, _)) => error!("Failed to enque message.\n {:?}", e),
            }
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                _ = self.interval.tick() => {
                    let intents = self.generate_positions();
                    self.send_intents(intents).await
                },
                agg = self.receiver.recv() => {
                    if let Some(agg) = agg {
                        self.update_price(agg)
                    }
                }
            }
        }
    }
}
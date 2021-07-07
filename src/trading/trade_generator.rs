use crate::trading::domain::Position;
use crate::trading::relay::RelayMessage;
use crate::trading::TradeBands;
use polygon::ws::Aggregate;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rust_decimal::prelude::*;
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{interval_at, Duration, Instant, Interval};
use tracing::{debug, error, info, trace, warn};
use trading_base::{AmountSpec, PositionIntent, TickerSpec, UpdatePolicy};

pub(super) struct TradeGenerator {
    cash: Decimal,
    pairs: Vec<TradeBands>,
    prices: HashMap<String, Decimal>,
    receiver: UnboundedReceiver<RelayMessage>,
    producer: FutureProducer,
    interval: Interval,
}

impl TradeGenerator {
    pub fn new(
        cash: Decimal,
        pairs: Vec<TradeBands>,
        receiver: UnboundedReceiver<RelayMessage>,
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

    #[tracing::instrument(skip(self))]
    fn generate_positions(&self) -> Vec<PositionIntent> {
        trace!("Generating positions");
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
                            .update_policy(UpdatePolicy::RetainLong)
                            .limit_price(p1 * Decimal::new(1005, 3))
                            .sub_strategy(pair_string.clone())
                            .build()
                            .expect("Always works"),
                        );
                        intents.push(
                            PositionIntent::builder(
                                "double-trouble".to_string(),
                                pair.asset_2.clone(),
                                AmountSpec::Dollars(-self.cash / Decimal::new(3, 0)),
                            )
                            .update_policy(UpdatePolicy::RetainShort)
                            .limit_price(p2 * Decimal::new(995, 3))
                            .sub_strategy(pair_string.clone())
                            .build()
                            .expect("Always works"),
                        );
                    }
                    Position::Short => {
                        intents.push(
                            PositionIntent::builder(
                                "double-trouble".to_string(),
                                pair.asset_1.clone(),
                                AmountSpec::Dollars(-self.cash / Decimal::new(3, 0)),
                            )
                            .update_policy(UpdatePolicy::RetainShort)
                            .limit_price(p1 * Decimal::new(995, 3))
                            .sub_strategy(pair_string.clone())
                            .build()
                            .expect("Always works"),
                        );
                        intents.push(
                            PositionIntent::builder(
                                "double-trouble".to_string(),
                                pair.asset_2.clone(),
                                AmountSpec::Dollars(self.cash / Decimal::new(3, 0)),
                            )
                            .update_policy(UpdatePolicy::RetainLong)
                            .limit_price(p2 * Decimal::new(1005, 3))
                            .sub_strategy(pair_string.clone())
                            .build()
                            .expect("Always works"),
                        );
                    }
                    Position::RetainLong => {
                        intents.push(
                            PositionIntent::builder(
                                "double-trouble".to_string(),
                                pair.asset_1.clone(),
                                AmountSpec::Zero,
                            )
                            .sub_strategy(pair_string.clone())
                            .update_policy(UpdatePolicy::RetainLong)
                            .build()
                            .expect("Always works"),
                        );
                        intents.push(
                            PositionIntent::builder(
                                "double-trouble".to_string(),
                                pair.asset_2.clone(),
                                AmountSpec::Zero,
                            )
                            .update_policy(UpdatePolicy::RetainShort)
                            .sub_strategy(pair_string.clone())
                            .build()
                            .expect("Always works"),
                        );
                    }
                    Position::RetainShort => {
                        intents.push(
                            PositionIntent::builder(
                                "double-trouble".to_string(),
                                pair.asset_1.clone(),
                                AmountSpec::Zero,
                            )
                            .update_policy(UpdatePolicy::RetainShort)
                            .sub_strategy(pair_string.clone())
                            .build()
                            .expect("Always works"),
                        );
                        intents.push(
                            PositionIntent::builder(
                                "double-trouble".to_string(),
                                pair.asset_2.clone(),
                                AmountSpec::Zero,
                            )
                            .update_policy(UpdatePolicy::RetainLong)
                            .sub_strategy(pair_string.clone())
                            .build()
                            .expect("Always works"),
                        );
                    }
                }
            }
        }
        intents
    }

    async fn send_intents(&self, intents: Vec<PositionIntent>) {
        for intent in intents {
            debug!("Sending intent {:?}", intent);
            let ticker = match intent.ticker.clone() {
                TickerSpec::Ticker(ticker) => ticker,
                _ => unreachable!(),
            };
            let payload = serde_json::to_vec(&intent).unwrap();
            let record = FutureRecord::to("position-intents")
                .key(&ticker)
                .payload(&payload);
            let res = self
                .producer
                .send(record, std::time::Duration::from_secs(0))
                .await;
            if let Err((e, msg)) = res {
                error!(
                    "Failed to send message.\nError: {:?}\nMessage: {:?}",
                    e, msg
                )
            }
        }
    }

    async fn wind_down(&mut self) {
        let intent = PositionIntent::builder("double-trouble", TickerSpec::All, AmountSpec::Zero)
            .build()
            .expect("Always works");
        debug!("Sending intent {:?}", intent);
        let payload = serde_json::to_vec(&intent).unwrap();
        let record = FutureRecord::to("position-intents")
            .key("")
            .payload(&payload);
        let res = self
            .producer
            .send(record, std::time::Duration::from_secs(0))
            .await;
        if let Err((e, msg)) = res {
            error!(
                "Failed to send message.\nError: {:?}\nMessage: {:?}",
                e, msg
            )
        }
    }

    pub async fn run(&mut self) {
        info!("Starting TradeGenerator");
        loop {
            tokio::select! {
                _ = self.interval.tick() => {
                    trace!("Tick");
                    let intents = self.generate_positions();
                    self.send_intents(intents).await
                },
                msg = self.receiver.recv() => {
                    match msg {
                        Some(RelayMessage::Agg(agg)) => {
                            self.update_price(agg)
                        },
                        Some(RelayMessage::WindDown) => {
                            self.wind_down().await;
                            return
                        }
                        None => {
                            warn!("Relay has shut down but OrderGenerator is still running");
                            return
                        }
                    }
                }
            }
        }
    }
}

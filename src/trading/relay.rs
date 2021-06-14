use futures::prelude::*;
use polygon::ws::{Aggregate, PolygonMessage};
use rdkafka::consumer::StreamConsumer;
use rdkafka::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, trace, warn};

#[derive(Deserialize, Serialize)]
#[serde(tag = "state", rename_all = "lowercase")]
enum State {
    Open { next_close: usize },
    Closed { next_open: usize },
}

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum Input {
    MarketState(State),
    Polygon(PolygonMessage),
}

#[derive(Debug)]
pub(crate) enum RelayMessage {
    Agg(Aggregate),
    WindDown,
}

pub(super) struct Relay {
    tickers: HashSet<String>,
    consumer: StreamConsumer,
    sender: UnboundedSender<RelayMessage>,
}

impl Relay {
    pub fn new(
        tickers: HashSet<String>,
        consumer: StreamConsumer,
        sender: UnboundedSender<RelayMessage>,
    ) -> Self {
        Self {
            tickers,
            consumer,
            sender,
        }
    }

    pub async fn run(&self) {
        self.consumer
            .stream()
            .filter_map(|message| async move {
                match message {
                    Ok(message) => Some(message),
                    Err(e) => {
                        error!("{:?}", e);
                        None
                    }
                }
            })
            .filter_map(|message| async move {
                message
                    .payload()
                    .map(|bytes| serde_json::from_slice::<Input>(bytes))
            })
            .filter_map(|res| async move {
                match res {
                    Ok(message) => Some(message),
                    Err(e) => {
                        error!("{:?}", e);
                        None
                    }
                }
            })
            .for_each_concurrent(50, |parsed| async move {
                match parsed {
                    Input::Polygon(PolygonMessage::Second(agg)) => {
                        if self.tickers.contains(&agg.symbol) {
                            trace!("{:?}", agg);
                            let res = self.sender.send(RelayMessage::Agg(agg));
                            if let Err(e) = res {
                                error!("{:?}", e);
                            }
                        }
                    }
                    Input::MarketState(State::Open { next_close }) => {
                        if next_close <= 600 {
                            let res = self.sender.send(RelayMessage::WindDown);
                            if let Err(e) = res {
                                error!("{:?}", e);
                            }
                            return;
                        }
                    }
                    Input::MarketState(State::Closed { .. }) => {
                        warn!("Markets are closed yet double-trouble is running")
                    }
                    Input::Polygon(_) => unreachable!(),
                }
            })
            .await
    }
}

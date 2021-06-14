use futures::prelude::*;
use polygon::ws::{Aggregate, PolygonMessage};
use rdkafka::consumer::StreamConsumer;
use rdkafka::Message;
use std::collections::HashSet;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, trace};

pub struct Relay {
    tickers: HashSet<String>,
    consumer: StreamConsumer,
    sender: UnboundedSender<Aggregate>,
}

impl Relay {
    pub fn new(
        tickers: HashSet<String>,
        consumer: StreamConsumer,
        sender: UnboundedSender<Aggregate>,
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
                    .map(|bytes| serde_json::from_slice::<PolygonMessage>(bytes))
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
                if let PolygonMessage::Second(agg) = parsed {
                    if self.tickers.contains(&agg.symbol) {
                        trace!("{:?}", agg);
                        let res = self.sender.send(agg);
                        if let Err(e) = res {
                            error!("{:?}", e);
                        }
                    }
                }
            })
            .await
    }
}

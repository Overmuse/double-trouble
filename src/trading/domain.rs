use crate::trading::data::TradePair;
use rust_decimal::prelude::*;
use tracing::info;

#[derive(Debug, Clone, PartialEq)]
pub enum Position {
    Long,
    RetainLong,
    RetainShort,
    Short,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TradeBands {
    pub asset_1: String,
    pub asset_2: String,
    pub upper_band: Decimal,
    pub equilibrium: Decimal,
    pub lower_band: Decimal,
    pub original_st_spread: Decimal,
}

impl TradeBands {
    pub fn new(trade_pair: TradePair, overnight_spread_change: Decimal) -> Self {
        let equilibrium =
            (overnight_spread_change - trade_pair.original_lt_spread) / Decimal::new(2, 0);
        let upper_band = equilibrium + trade_pair.epsilon;
        let lower_band = equilibrium - trade_pair.epsilon;
        Self {
            asset_1: trade_pair.asset_1,
            asset_2: trade_pair.asset_2,
            upper_band,
            equilibrium,
            lower_band,
            original_st_spread: trade_pair.original_st_spread,
        }
    }

    #[tracing::instrument]
    pub fn trade_signal(&self, price_1: &Decimal, price_2: &Decimal) -> Position {
        let spread = (price_1.ln() - price_2.ln()) - self.original_st_spread;
        if spread > self.upper_band {
            info!("Upper band breached, going short");
            Position::Short
        } else if spread > self.equilibrium {
            Position::RetainShort
        } else if spread < self.lower_band {
            info!("Lower band breached, going long");
            Position::Long
        } else {
            Position::RetainLong
        }
    }
}

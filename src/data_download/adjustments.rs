use crate::data_download::{DividendData, PriceData, SplitData};
use chrono::prelude::*;
use rust_decimal::prelude::*;

fn dividend_adjustments(
    prices: &[(DateTime<Utc>, Decimal)],
    dividends: &[(NaiveDate, Decimal)],
) -> Vec<(NaiveDate, Decimal)> {
    dividends
        .iter()
        .map(|div| {
            prices
                .iter()
                .zip(prices.iter().skip(1))
                .find(|((_, _), (d2, _))| d2.naive_utc().date() == div.0)
                .map(|((_, p1), (_, _))| (div.0, Decimal::new(1, 0) - (div.1 / p1)))
        })
        .flatten()
        .collect()
}

fn adjustments(
    prices: &[(DateTime<Utc>, Decimal)],
    dividends: &[(NaiveDate, Decimal)],
    splits: &[(NaiveDate, Decimal)],
) -> Vec<(NaiveDate, Decimal)> {
    let mut adjustments = dividend_adjustments(prices, dividends);
    adjustments.extend_from_slice(splits);
    adjustments.sort_by_key(|x| x.0);
    adjustments
}

fn cumulative_adjustments(adjustments: &[(NaiveDate, Decimal)]) -> Vec<(NaiveDate, Decimal)> {
    let mut v: Vec<(NaiveDate, Decimal)> = adjustments
        .iter()
        .rev()
        .scan(Decimal::new(1, 0), |state, &(date, adj)| {
            *state *= adj;
            Some((date, *state))
        })
        .collect();
    v.reverse();
    v
}

pub fn adjust_prices(
    price_data: PriceData,
    dividend_data: DividendData,
    split_data: SplitData,
) -> PriceData {
    price_data
        .keys()
        .map(|ticker| {
            let prices = price_data.get(ticker).unwrap();
            let dividends = dividend_data.get(ticker).unwrap();
            let splits = split_data.get(ticker).unwrap();
            let adjustments = adjustments(prices, dividends, splits);
            let cumulative = cumulative_adjustments(&adjustments);
            let adjusted_prices: Vec<(DateTime<Utc>, Decimal)> = prices
                .iter()
                .scan(
                    cumulative.iter().peekable(),
                    |state, (price_date, price)| {
                        if let Some((adj_date, adj)) = state.peek() {
                            if &price_date.naive_utc().date() < adj_date {
                                Some((*price_date, price * adj))
                            } else {
                                (*state).next();
                                if let Some((_, adj)) = state.peek() {
                                    Some((*price_date, price * adj))
                                } else {
                                    Some((*price_date, *price))
                                }
                            }
                        } else {
                            Some((*price_date, *price))
                        }
                    },
                )
                .collect();
            (ticker.clone(), adjusted_prices)
        })
        .collect()
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_dividend_adjustments() {
        let prices = vec![
            (Utc.ymd(2021, 1, 1).and_hms(0, 0, 0), Decimal::new(10000, 2)),
            (Utc.ymd(2021, 1, 2).and_hms(0, 0, 0), Decimal::new(9000, 2)),
        ];
        let dividends = vec![(NaiveDate::from_ymd(2021, 1, 2), Decimal::new(1000, 2))];

        let adjustments = dividend_adjustments(&prices, &dividends);
        assert_eq!(
            adjustments,
            vec![(NaiveDate::from_ymd(2021, 1, 2), Decimal::new(9, 1))]
        );
    }

    #[test]
    fn test_adjustments() {
        let prices = vec![
            (Utc.ymd(2021, 1, 1).and_hms(0, 0, 0), Decimal::new(10000, 2)),
            (Utc.ymd(2021, 1, 2).and_hms(0, 0, 0), Decimal::new(9000, 2)),
            (Utc.ymd(2021, 1, 3).and_hms(0, 0, 0), Decimal::new(4500, 2)),
        ];
        let dividends = vec![(NaiveDate::from_ymd(2021, 1, 2), Decimal::new(1000, 2))];
        let splits = vec![(NaiveDate::from_ymd(2021, 1, 3), Decimal::new(5, 1))];

        let adjustments = adjustments(&prices, &dividends, &splits);
        assert_eq!(
            adjustments,
            vec![
                (NaiveDate::from_ymd(2021, 1, 2), Decimal::new(9, 1)),
                (NaiveDate::from_ymd(2021, 1, 3), Decimal::new(5, 1))
            ]
        );
    }

    #[test]
    fn test_cumulative_adjustments() {
        let adjustments = vec![
            (NaiveDate::from_ymd(2021, 1, 2), Decimal::new(9, 1)),
            (NaiveDate::from_ymd(2021, 1, 3), Decimal::new(5, 1)),
        ];
        let cumulative = cumulative_adjustments(&adjustments);
        assert_eq!(
            cumulative,
            vec![
                (NaiveDate::from_ymd(2021, 1, 2), Decimal::new(45, 2)),
                (NaiveDate::from_ymd(2021, 1, 3), Decimal::new(5, 1))
            ]
        );
    }

    #[test]
    fn test_adjust_prices() {
        let mut prices = HashMap::new();
        prices.insert(
            "AAPL".to_string(),
            vec![
                (Utc.ymd(2021, 1, 1).and_hms(0, 0, 0), Decimal::new(10000, 2)),
                (Utc.ymd(2021, 1, 2).and_hms(0, 0, 0), Decimal::new(9000, 2)),
                (Utc.ymd(2021, 1, 3).and_hms(0, 0, 0), Decimal::new(4500, 2)),
            ],
        );
        let mut dividends = HashMap::new();
        dividends.insert(
            "AAPL".to_string(),
            vec![(NaiveDate::from_ymd(2021, 1, 2), Decimal::new(1000, 2))],
        );
        let mut splits = HashMap::new();
        splits.insert(
            "AAPL".to_string(),
            vec![(NaiveDate::from_ymd(2021, 1, 3), Decimal::new(5, 1))],
        );
        let adjusted_prices = adjust_prices(prices, dividends, splits);
        assert_eq!(
            adjusted_prices.get("AAPL"),
            Some(&vec![
                (Utc.ymd(2021, 1, 1).and_hms(0, 0, 0), Decimal::new(4500, 2)),
                (Utc.ymd(2021, 1, 2).and_hms(0, 0, 0), Decimal::new(4500, 2)),
                (Utc.ymd(2021, 1, 3).and_hms(0, 0, 0), Decimal::new(4500, 2)),
            ]),
        )
    }
}

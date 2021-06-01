use bdays::{calendars::us::USSettlement, HolidayCalendar};
use chrono::{DateTime, NaiveDate, Utc};
use futures::future::join_all;
use polygon::rest::{Client, GetAggregate, Timespan};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::iter::once;

pub type PriceData = HashMap<String, Vec<(DateTime<Utc>, Decimal)>>;

async fn download_ticker_price_data(
    client: &Client<'_>,
    ticker: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> (String, Vec<(DateTime<Utc>, Decimal)>) {
    let cal = USSettlement;
    let end_1 = cal.advance_bdays(end_date, -67);
    let start_2 = cal.advance_bdays(end_date, -66);
    let end_2 = cal.advance_bdays(end_date, -34);
    let start_3 = cal.advance_bdays(end_date, -33);
    // Split into three queries to get around the data limits.
    // TODO: Build this into the api somehow, perhaps with pagination?
    let queries = once(
        GetAggregate::new(ticker, start_date, end_1)
            .multiplier(5)
            .timespan(Timespan::Minute)
            .unadjusted(true)
            .limit(50000),
    )
    .chain(once(
        GetAggregate::new(ticker, start_2, end_2)
            .multiplier(5)
            .timespan(Timespan::Minute)
            .unadjusted(true)
            .limit(50000),
    ))
    .chain(once(
        GetAggregate::new(ticker, start_3, end_date)
            .multiplier(5)
            .timespan(Timespan::Minute)
            .unadjusted(true)
            .limit(50000),
    ));

    let data: Vec<(DateTime<Utc>, Decimal)> = client
        .send_all(queries)
        .await
        .into_iter()
        .filter_map(|res| res.ok())
        .filter_map(|wrapper| wrapper.results)
        .flat_map(|aggs| {
            let data: Vec<(DateTime<Utc>, Decimal)> =
                aggs.iter().map(|agg| (agg.t, agg.c)).collect();
            data
        })
        .collect();

    (ticker.to_string(), data)
}

pub async fn download_price_data(
    client: &Client<'_>,
    tickers: &[&str],
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> PriceData {
    let futs = tickers
        .iter()
        .map(|ticker| download_ticker_price_data(client, ticker, start_date, end_date));
    join_all(futs).await.into_iter().collect()
}

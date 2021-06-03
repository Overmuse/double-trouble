use config::{Config, ConfigError, Environment};
use kafka_settings::KafkaSettings;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer};

#[derive(Debug, Deserialize)]
#[serde(tag = "run_mode", rename_all = "snake_case")]
pub enum RunMode {
    Download { out_file: String },
    Run { data_file: String },
}

#[derive(Debug, Deserialize)]
pub struct AppSettings {
    pub cash: Decimal,
    #[serde(flatten)]
    pub run_mode: RunMode,
    #[serde(deserialize_with = "vec_from_str", default)]
    pub tickers: Vec<String>,
}

pub fn vec_from_str<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(s.split(',').map(From::from).collect())
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub app: AppSettings,
    pub kafka: KafkaSettings,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::new();
        s.merge(Environment::new().separator("__"))?;
        s.try_into()
    }
}

pub struct AssetModel<'a> {
    pub ticker: &'a str,
    pub reversion_level: f64,
    pub upper_band: f64,
    pub lower_band: f64,
}

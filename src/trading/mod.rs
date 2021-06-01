use crate::asset_selection::AssetModel;

#[derive(Debug, Clone, PartialEq)]
enum Position {
    Long,
    Short,
}

fn trade_signal(
    model: &AssetModel,
    current_position: Option<Position>,
    price: f64,
) -> Option<Position> {
    match (current_position, price) {
        (None, p) if p >= model.upper_band => Some(Position::Short),
        (None, p) if p <= model.lower_band => Some(Position::Long),
        (None, _) => None,
        (Some(Position::Long), p) if p >= model.reversion_level => None,
        (Some(Position::Short), p) if p <= model.reversion_level => None,
        (Some(pos), _) => Some(pos),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    mod trade_signal {
        use super::*;
        const MODEL: AssetModel = AssetModel {
            ticker: "A",
            lower_band: 90.0,
            reversion_level: 100.0,
            upper_band: 110.0,
        };

        #[test]
        fn significant_positive_deviation_should_lead_to_short() {
            let signal = trade_signal(&MODEL, None, 120.0);
            assert_eq!(signal, Some(Position::Short));
        }

        #[test]
        fn insignificant_positive_deviation_should_not_lead_to_short() {
            let signal = trade_signal(&MODEL, None, 109.0);
            assert_eq!(signal, None);
        }

        #[test]
        fn significant_negative_deviation_should_lead_to_long() {
            let signal = trade_signal(&MODEL, None, 80.0);
            assert_eq!(signal, Some(Position::Long));
        }

        #[test]
        fn insignificant_negative_deviation_should_not_lead_to_long() {
            let signal = trade_signal(&MODEL, None, 91.0);
            assert_eq!(signal, None);
        }

        #[test]
        fn reversion_should_lead_to_no_position() {
            let signal = trade_signal(&MODEL, Some(Position::Long), 101.0);
            assert_eq!(signal, None);

            let signal = trade_signal(&MODEL, Some(Position::Short), 99.0);
            assert_eq!(signal, None);
        }

        #[test]
        fn insufficient_reversion_should_retain_position() {
            let signal = trade_signal(&MODEL, Some(Position::Long), 99.0);
            assert_eq!(signal, Some(Position::Long));

            let signal = trade_signal(&MODEL, Some(Position::Short), 101.0);
            assert_eq!(signal, Some(Position::Short));
        }
    }
}

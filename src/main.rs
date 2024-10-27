use chrono::{Datelike, NaiveDate, NaiveDateTime};
use datafusion::arrow::array::*;
use datafusion_common::error::DataFusionError;
use datafusion_functions_nested::greatest::greatest_inner;
use std::sync::Arc;

fn main() -> std::result::Result<(), DataFusionError> {
    // Test with Int32Array
    let input_int = vec![
        Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
        Arc::new(Int32Array::from(vec![Some(4), Some(5), Some(6)])) as ArrayRef,
        Arc::new(Int32Array::from(vec![Some(7), Some(1), Some(2)])) as ArrayRef,
    ];

    let greatest_int = greatest_inner(&input_int)?;
    let greatest_int_array = greatest_int
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("Failed to downcast to Int32Array");
    println!("Greatest Int: {:?}", greatest_int_array);

    // Test with Float64Array
    let input_float = vec![
        Arc::new(Float64Array::from(vec![Some(1.1), Some(2.2), None])) as ArrayRef,
        Arc::new(Float64Array::from(vec![Some(3.3), Some(1.1), Some(4.4)])) as ArrayRef,
    ];

    let greatest_float = greatest_inner(&input_float)?;
    let greatest_float_array = greatest_float
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Failed to downcast to Float64Array");
    println!("Greatest Float: {:?}", greatest_float_array);

    // Test with BooleanArray
    let input_bool = vec![
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as ArrayRef,
        Arc::new(BooleanArray::from(vec![
            Some(false),
            Some(true),
            Some(true),
        ])) as ArrayRef,
    ];

    let greatest_bool = greatest_inner(&input_bool)?;
    let greatest_bool_array = greatest_bool
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("Failed to downcast to BooleanArray");
    println!("Greatest Boolean: {:?}", greatest_bool_array);

    // Test with StringArray
    let input_utf8 = vec![
        Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])) as ArrayRef,
        Arc::new(StringArray::from(vec![Some("d"), Some("e"), Some("f")])) as ArrayRef,
        Arc::new(StringArray::from(vec![Some("g"), Some("h"), Some("i")])) as ArrayRef,
    ];

    let greatest_utf8 = greatest_inner(&input_utf8)?;
    let greatest_utf8_array = greatest_utf8
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Failed to downcast to StringArray");
    println!("Greatest UTF8: {:?}", greatest_utf8_array);

    // Helper function to create Date32Array from date strings
    fn date32_array_from_dates(dates: &[Option<&str>]) -> Date32Array {
        dates
            .iter()
            .map(|opt| {
                opt.map(|s| {
                    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap();
                    date.num_days_from_ce() - NaiveDate::from_ymd(1970, 1, 1).num_days_from_ce()
                })
            })
            .collect()
    }

    // Test with Date32Array
    let input_dates = vec![
        Arc::new(date32_array_from_dates(&[
            Some("2020-01-01"),
            Some("2020-06-01"),
            None,
        ])) as ArrayRef,
        Arc::new(date32_array_from_dates(&[
            Some("2020-03-01"),
            Some("2020-05-01"),
            Some("2020-07-01"),
        ])) as ArrayRef,
    ];

    let greatest_dates = greatest_inner(&input_dates)?;
    let greatest_dates_array = greatest_dates
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("Failed to downcast to Date32Array");
    println!("Greatest Dates: {:?}", greatest_dates_array);

    // Helper function to create TimestampNanosecondArray from datetime strings
    fn timestamp_ns_array_from_datetimes(datetimes: &[Option<&str>]) -> TimestampNanosecondArray {
        datetimes
            .iter()
            .map(|opt| {
                opt.map(|s| {
                    let datetime = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap();
                    datetime.timestamp() * 1_000_000_000 + datetime.timestamp_subsec_nanos() as i64
                })
            })
            .collect()
    }

    // Test with TimestampNanosecondArray
    let input_timestamps = vec![
        Arc::new(timestamp_ns_array_from_datetimes(&[
            Some("2020-01-01 12:00:00"),
            Some("2020-06-01 08:00:00"),
            None,
        ])) as ArrayRef,
        Arc::new(timestamp_ns_array_from_datetimes(&[
            Some("2020-03-01 15:30:00"),
            Some("2020-05-01 20:45:00"),
            Some("2020-07-01 10:15:00"),
        ])) as ArrayRef,
    ];

    let greatest_timestamps = greatest_inner(&input_timestamps)?;
    let greatest_timestamps_array = greatest_timestamps
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("Failed to downcast to TimestampNanosecondArray");
    println!("Greatest Timestamps: {:?}", greatest_timestamps_array);

    Ok(())
}

//! The only data plane is Arrow. Driver values exist only while decoding/encoding.
use crate::{
    config::{Mapping, offset_seconds},
    error::{Error, Result},
};
use arrow::{array::*, datatypes::*, record_batch::RecordBatch};
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use mysql_async::{
    Column, Value,
    consts::{ColumnFlags, ColumnType},
};
use std::{collections::HashMap, str::FromStr, sync::Arc};

pub fn type_error(msg: impl Into<String>) -> Error {
    Error::new("TYPE", msg)
}
pub fn mysql_schema(columns: &[Column]) -> Result<SchemaRef> {
    let fields = columns
        .iter()
        .map(|c| {
            let unsigned = c.flags().contains(ColumnFlags::UNSIGNED_FLAG);
            let nullable = !c.flags().contains(ColumnFlags::NOT_NULL_FLAG);
            let typ = match c.column_type() {
                ColumnType::MYSQL_TYPE_TINY => {
                    if unsigned {
                        DataType::UInt8
                    } else {
                        DataType::Int8
                    }
                }
                ColumnType::MYSQL_TYPE_SHORT => {
                    if unsigned {
                        DataType::UInt16
                    } else {
                        DataType::Int16
                    }
                }
                ColumnType::MYSQL_TYPE_INT24 | ColumnType::MYSQL_TYPE_LONG => {
                    if unsigned {
                        DataType::UInt32
                    } else {
                        DataType::Int32
                    }
                }
                ColumnType::MYSQL_TYPE_LONGLONG => {
                    if unsigned {
                        DataType::UInt64
                    } else {
                        DataType::Int64
                    }
                }
                ColumnType::MYSQL_TYPE_YEAR => DataType::UInt16,
                ColumnType::MYSQL_TYPE_FLOAT => DataType::Float32,
                ColumnType::MYSQL_TYPE_DOUBLE => DataType::Float64,
                ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
                    let scale = c.decimals();
                    let precision = c
                        .column_length()
                        .saturating_sub(u32::from(!unsigned) + u32::from(scale > 0))
                        .clamp(1, 65) as u8;
                    if precision <= 38 {
                        DataType::Decimal128(precision, scale as i8)
                    } else {
                        DataType::Decimal256(precision, scale as i8)
                    }
                }
                ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => DataType::Date32,
                ColumnType::MYSQL_TYPE_DATETIME | ColumnType::MYSQL_TYPE_DATETIME2 => {
                    DataType::Timestamp(TimeUnit::Microsecond, None)
                }
                ColumnType::MYSQL_TYPE_TIMESTAMP | ColumnType::MYSQL_TYPE_TIMESTAMP2 => {
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
                }
                ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => {
                    DataType::Duration(TimeUnit::Microsecond)
                }
                ColumnType::MYSQL_TYPE_NULL => DataType::Null,
                ColumnType::MYSQL_TYPE_JSON => DataType::Utf8,
                ColumnType::MYSQL_TYPE_BIT => DataType::Binary,
                ColumnType::MYSQL_TYPE_STRING
                | ColumnType::MYSQL_TYPE_VAR_STRING
                | ColumnType::MYSQL_TYPE_VARCHAR
                | ColumnType::MYSQL_TYPE_BLOB
                | ColumnType::MYSQL_TYPE_TINY_BLOB
                | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
                | ColumnType::MYSQL_TYPE_LONG_BLOB
                | ColumnType::MYSQL_TYPE_ENUM
                | ColumnType::MYSQL_TYPE_SET => {
                    if c.character_set() == 63 {
                        DataType::Binary
                    } else {
                        DataType::Utf8
                    }
                }
                other => {
                    return Err(type_error(format!(
                        "Unsupported MySQL type {other:?}; project an explicit SQL conversion"
                    )));
                }
            };
            let mut metadata = HashMap::new();
            metadata.insert(
                "dunnelean.mysql_type".into(),
                format!("{:?}", c.column_type()),
            );
            Ok(Field::new(c.name_str().as_ref(), typ, nullable).with_metadata(metadata))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}
pub fn sql_type(
    sql: &str,
    precision: Option<u64>,
    scale: Option<u64>,
    datetime_precision: Option<u64>,
) -> Result<DataType> {
    let s = sql.to_ascii_lowercase();
    let base = s.split(['(', ' ']).next().unwrap_or("");
    let u = s.contains("unsigned");
    Ok(match base {
        "boolean" | "bool" => DataType::Boolean,
        "tinyint" => {
            if u {
                DataType::UInt8
            } else {
                DataType::Int8
            }
        }
        "smallint" => {
            if u {
                DataType::UInt16
            } else {
                DataType::Int16
            }
        }
        "mediumint" | "int" | "integer" => {
            if u {
                DataType::UInt32
            } else {
                DataType::Int32
            }
        }
        "bigint" => {
            if u {
                DataType::UInt64
            } else {
                DataType::Int64
            }
        }
        "largeint" => DataType::Decimal256(39, 0),
        "year" => DataType::UInt16,
        "float" => DataType::Float32,
        "double" | "real" => DataType::Float64,
        "decimal" | "decimalv3" | "decimalv2" => {
            let p = precision.unwrap_or(38) as u8;
            let sc = scale.unwrap_or(0) as i8;
            if p <= 38 {
                DataType::Decimal128(p, sc)
            } else {
                DataType::Decimal256(p, sc)
            }
        }
        "date" | "datev2" => DataType::Date32,
        "datetime" | "datetimev2" => DataType::Timestamp(
            if datetime_precision.unwrap_or(6) == 0 {
                TimeUnit::Second
            } else {
                TimeUnit::Microsecond
            },
            None,
        ),
        "timestamp" | "timestamptz" => DataType::Timestamp(
            if datetime_precision.unwrap_or(6) == 0 {
                TimeUnit::Second
            } else {
                TimeUnit::Microsecond
            },
            Some("UTC".into()),
        ),
        "time" => DataType::Duration(TimeUnit::Microsecond),
        "varchar" | "char" | "text" | "tinytext" | "mediumtext" | "longtext" | "string"
        | "json" | "enum" | "set" => DataType::Utf8,
        "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" | "bit" => {
            DataType::Binary
        }
        _ => {
            return Err(type_error(format!(
                "Unsupported SQL type {sql}; select an explicit SQL conversion"
            )));
        }
    })
}
pub fn decimal_parse(value: &str, precision: u8, scale: i8) -> Result<i256> {
    if scale < 0 {
        return Err(type_error("Negative decimal scale is unsupported"));
    }
    let (negative, s) = if let Some(s) = value.strip_prefix('-') {
        (true, s)
    } else {
        (false, value.strip_prefix('+').unwrap_or(value))
    };
    let parts = s.split('.').collect::<Vec<_>>();
    if parts.len() > 2
        || parts[0].is_empty()
        || !parts.iter().all(|s| s.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err(type_error("Invalid decimal"));
    }
    let fraction = parts.get(1).copied().unwrap_or("");
    if fraction.len() > scale as usize && fraction[scale as usize..].bytes().any(|b| b != b'0') {
        return Err(type_error("Decimal scale would lose precision"));
    }
    let mut digits = parts[0].to_owned();
    digits.push_str(&fraction[..fraction.len().min(scale as usize)]);
    digits.extend(std::iter::repeat_n(
        '0',
        (scale as usize).saturating_sub(fraction.len()),
    ));
    let digits = digits.trim_start_matches('0');
    if digits.len() > precision as usize {
        return Err(type_error(format!("Decimal precision exceeds {precision}")));
    }
    i256::from_str(&format!(
        "{}{}",
        if negative { "-" } else { "" },
        if digits.is_empty() { "0" } else { digits }
    ))
    .map_err(|_| type_error("Decimal256 overflow"))
}
pub fn decimal_string(value: impl ToString, scale: i8) -> String {
    let value = value.to_string();
    let neg = value.starts_with('-');
    let digits = value.trim_start_matches('-');
    if scale <= 0 {
        return value;
    }
    let digits = format!(
        "{}{}",
        "0".repeat((scale as usize + 1).saturating_sub(digits.len())),
        digits
    );
    let index = digits.len() - scale as usize;
    format!(
        "{}{}.{}",
        if neg { "-" } else { "" },
        &digits[..index],
        &digits[index..]
    )
}
fn date(y: u16, m: u8, d: u8) -> Result<NaiveDate> {
    NaiveDate::from_ymd_opt(y as i32, m as u32, d as u32)
        .ok_or_else(|| type_error("Zero/invalid SQL date"))
}
fn timestamp(value: &Value, offset: i32, aware: bool) -> Result<i64> {
    if let Value::Date(y, m, d, h, mi, s, us) = value {
        let dt = date(*y, *m, *d)?
            .and_hms_micro_opt(*h as u32, *mi as u32, *s as u32, *us)
            .ok_or_else(|| type_error("Invalid SQL datetime"))?;
        Ok(dt.and_utc().timestamp_micros() - if aware { offset as i64 * 1_000_000 } else { 0 })
    } else {
        Err(type_error("Expected datetime"))
    }
}
pub struct MysqlBatchBuilder {
    schema: SchemaRef,
    builders: Vec<Box<dyn ArrayBuilder>>,
    rows: usize,
    bytes: usize,
    offset: i32,
}
impl MysqlBatchBuilder {
    pub fn new(schema: SchemaRef, time_zone: &str) -> Result<Self> {
        let builders = schema
            .fields()
            .iter()
            .map(|f| make_builder(f.data_type(), 0))
            .collect();
        Ok(Self {
            schema,
            builders,
            rows: 0,
            bytes: 0,
            offset: offset_seconds(time_zone)?,
        })
    }
    pub fn rows(&self) -> usize {
        self.rows
    }
    pub fn bytes(&self) -> usize {
        self.bytes
    }
    pub fn row_bytes(row: &mysql_async::Row) -> usize {
        row.columns_ref()
            .iter()
            .enumerate()
            .map(|(i, _)| match row.as_ref(i) {
                Some(Value::Bytes(b)) => b.len() + 16,
                _ => 32,
            })
            .sum()
    }
    pub fn append(&mut self, row: mysql_async::Row) -> Result<()> {
        self.bytes += Self::row_bytes(&row);
        for (i, (field, builder)) in self
            .schema
            .fields()
            .iter()
            .zip(self.builders.iter_mut())
            .enumerate()
        {
            let value = row.as_ref(i).ok_or_else(|| type_error("Missing column"))?;
            macro_rules! push {
                ($builder:ty,$value:expr) => {{
                    let b = builder
                        .as_any_mut()
                        .downcast_mut::<$builder>()
                        .expect("matching builder");
                    if matches!(value, Value::NULL) {
                        b.append_null();
                    } else {
                        b.append_value($value);
                    }
                }};
            }
            macro_rules! integer {
                ($builder:ty,$ty:ty) => {
                    push!(
                        $builder,
                        match value {
                            Value::Int(v) =>
                                <$ty>::try_from(*v).map_err(|_| type_error("Integer overflow"))?,
                            Value::UInt(v) =>
                                <$ty>::try_from(*v).map_err(|_| type_error("Integer overflow"))?,
                            _ => return Err(type_error("Expected integer")),
                        }
                    )
                };
            }
            match field.data_type() {
                DataType::Int8 => integer!(Int8Builder, i8),
                DataType::Int16 => integer!(Int16Builder, i16),
                DataType::Int32 => integer!(Int32Builder, i32),
                DataType::Int64 => integer!(Int64Builder, i64),
                DataType::UInt8 => integer!(UInt8Builder, u8),
                DataType::UInt16 => integer!(UInt16Builder, u16),
                DataType::UInt32 => integer!(UInt32Builder, u32),
                DataType::UInt64 => integer!(UInt64Builder, u64),
                DataType::Float32 => push!(
                    Float32Builder,
                    match value {
                        Value::Float(v) => *v,
                        _ => return Err(type_error("Expected float32")),
                    }
                ),
                DataType::Float64 => push!(
                    Float64Builder,
                    match value {
                        Value::Double(v) => *v,
                        _ => return Err(type_error("Expected float64")),
                    }
                ),
                DataType::Utf8 => push!(
                    StringBuilder,
                    match value {
                        Value::Bytes(b) =>
                            std::str::from_utf8(b).map_err(|_| type_error("Invalid UTF-8"))?,
                        _ => return Err(type_error("Expected string")),
                    }
                ),
                DataType::Binary => push!(
                    BinaryBuilder,
                    match value {
                        Value::Bytes(b) => b.as_slice(),
                        _ => return Err(type_error("Expected binary")),
                    }
                ),
                DataType::Decimal128(p, s) => push!(
                    Decimal128Builder,
                    match value {
                        Value::Bytes(b) => decimal_parse(
                            std::str::from_utf8(b).map_err(|_| type_error("Invalid decimal"))?,
                            *p,
                            *s
                        )?
                        .to_i128()
                        .ok_or_else(|| type_error("Decimal128 overflow"))?,
                        _ => return Err(type_error("Expected decimal")),
                    }
                ),
                DataType::Decimal256(p, s) => push!(
                    Decimal256Builder,
                    match value {
                        Value::Bytes(b) => decimal_parse(
                            std::str::from_utf8(b).map_err(|_| type_error("Invalid decimal"))?,
                            *p,
                            *s
                        )?,
                        _ => return Err(type_error("Expected decimal")),
                    }
                ),
                DataType::Date32 => push!(
                    Date32Builder,
                    match value {
                        Value::Date(y, m, d, _, _, _, _) => date(*y, *m, *d)?
                            .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                            .num_days()
                            as i32,
                        _ => return Err(type_error("Expected date")),
                    }
                ),
                DataType::Timestamp(TimeUnit::Microsecond, tz) => push!(
                    TimestampMicrosecondBuilder,
                    timestamp(value, self.offset, tz.is_some())?
                ),
                DataType::Duration(TimeUnit::Microsecond) => push!(
                    DurationMicrosecondBuilder,
                    match value {
                        Value::Time(neg, d, h, m, s, us) =>
                            (if *neg { -1 } else { 1 })
                                * (((*d as i64 * 24 + *h as i64) * 3600
                                    + *m as i64 * 60
                                    + *s as i64)
                                    * 1_000_000
                                    + *us as i64),
                        _ => return Err(type_error("Expected MySQL TIME duration")),
                    }
                ),
                DataType::Null => builder
                    .as_any_mut()
                    .downcast_mut::<NullBuilder>()
                    .unwrap()
                    .append_null(),
                other => return Err(type_error(format!("Unsupported builder {other:?}"))),
            }
        }
        self.rows += 1;
        Ok(())
    }
    pub fn finish(&mut self) -> Result<RecordBatch> {
        let arrays = self.builders.iter_mut().map(|b| b.finish()).collect();
        self.rows = 0;
        self.bytes = 0;
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}
#[derive(Clone, Debug)]
pub struct TargetColumn {
    pub field: Field,
    pub sql_type: String,
    pub has_default: bool,
    pub generated: bool,
    pub key: bool,
    pub datetime_precision: Option<u64>,
}
#[derive(Clone)]
pub struct BatchMapping {
    pub target: SchemaRef,
    pub indices: Vec<usize>,
    pub columns: Vec<TargetColumn>,
    pub time_zone: String,
}
impl BatchMapping {
    pub fn plan(
        source: &Schema,
        target: &[TargetColumn],
        mapping: &[Mapping],
        time_zone: &str,
    ) -> Result<Self> {
        let mapping = if mapping.is_empty() {
            source
                .fields()
                .iter()
                .map(|f| Mapping {
                    source: f.name().clone(),
                    target: f.name().clone(),
                })
                .collect()
        } else {
            mapping.to_vec()
        };
        let mut columns = Vec::new();
        let mut indices = Vec::new();
        for m in &mapping {
            let index = source.index_of(&m.source).map_err(|_| {
                Error::new("SCHEMA", format!("Source column {} not found", m.source))
            })?;
            let col = target
                .iter()
                .find(|t| t.field.name() == &m.target)
                .ok_or_else(|| {
                    Error::new("SCHEMA", format!("Target column {} not found", m.target))
                })?;
            if col.generated {
                return Err(Error::new(
                    "SCHEMA",
                    format!("Cannot write generated column {}", m.target),
                ));
            }
            let src = source.field(index).data_type();
            let dst = col.field.data_type();
            let compatible = src == dst
                || matches!(src, DataType::Null)
                || numeric(src) && numeric(dst)
                || matches!(src, DataType::Boolean) && numeric(dst)
                || numeric(src) && matches!(dst, DataType::Boolean)
                || matches!(
                    (src, dst),
                    (DataType::Timestamp(_, _), DataType::Timestamp(_, _))
                        | (DataType::LargeUtf8, DataType::Utf8)
                        | (DataType::LargeBinary, DataType::Binary)
                );
            if !compatible {
                return Err(type_error(format!(
                    "{}: incompatible {src:?} → {dst:?}; convert explicitly in source SQL",
                    m.target
                )));
            }
            columns.push(col.clone());
            indices.push(index);
        }
        for col in target {
            if !col.generated
                && !col.has_default
                && !col.field.is_nullable()
                && !columns.iter().any(|c| c.field.name() == col.field.name())
            {
                return Err(Error::new(
                    "SCHEMA",
                    format!("Required target column {} is missing", col.field.name()),
                ));
            }
        }
        Ok(Self {
            target: Arc::new(Schema::new(
                columns.iter().map(|c| c.field.clone()).collect::<Vec<_>>(),
            )),
            indices,
            columns,
            time_zone: time_zone.into(),
        })
    }
    pub fn apply(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let arrays = self
            .indices
            .iter()
            .zip(&self.columns)
            .map(|(index, col)| {
                let a = batch.column(*index);
                let target = col.field.data_type();
                let converted = checked_cast(a, target, &self.time_zone)?;
                if !col.field.is_nullable() && converted.null_count() > 0 {
                    return Err(type_error(format!(
                        "NULL in required column {}",
                        col.field.name()
                    )));
                }
                if let Some(p) = col.datetime_precision.filter(|p| *p < 6)
                    && let DataType::Timestamp(TimeUnit::Microsecond, _) = target
                {
                    let a = converted
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    let factor = 10_i64.pow(6 - p as u32);
                    if a.iter().flatten().any(|v| v % factor != 0) {
                        return Err(type_error(format!(
                            "Timestamp precision loss in {}",
                            col.field.name()
                        )));
                    }
                }
                Ok(converted)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(RecordBatch::try_new(self.target.clone(), arrays)?)
    }
}
fn numeric(t: &DataType) -> bool {
    t.is_numeric()
}
pub fn checked_cast(a: &ArrayRef, target: &DataType, time_zone: &str) -> Result<ArrayRef> {
    if a.data_type() == target {
        return Ok(a.clone());
    }
    if matches!(a.data_type(), DataType::Null) {
        return Ok(new_null_array(target, a.len()));
    }
    if let (DataType::Timestamp(_, from), DataType::Timestamp(_, to)) = (a.data_type(), target) {
        let as_us =
            arrow::compute::cast(a, &DataType::Timestamp(TimeUnit::Microsecond, from.clone()))?;
        let arr = as_us
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let delta = offset_seconds(time_zone)? as i64
            * 1_000_000
            * match (from.is_some(), to.is_some()) {
                (true, false) => 1,
                (false, true) => -1,
                _ => 0,
            };
        let shifted: TimestampMicrosecondArray = arr
            .iter()
            .map(|v| v.and_then(|v| v.checked_add(delta)))
            .collect();
        if shifted.null_count() != a.null_count() {
            return Err(type_error("Timestamp overflow"));
        }
        let shifted: ArrayRef = Arc::new(shifted.with_timezone_opt(to.clone()));
        let out = arrow::compute::cast(&shifted, target)?;
        let back = arrow::compute::cast(&out, shifted.data_type())?;
        if shifted.to_data() != back.to_data() {
            return Err(type_error("Timestamp precision loss"));
        }
        return Ok(out);
    }
    let out = arrow::compute::cast(a, target)?;
    if out.null_count() != a.null_count() {
        return Err(type_error(format!(
            "Overflow converting {:?} to {target:?}",
            a.data_type()
        )));
    }
    let back = arrow::compute::cast(&out, a.data_type())?;
    if back.to_data() != a.to_data() {
        return Err(type_error(format!(
            "Precision loss converting {:?} to {target:?}",
            a.data_type()
        )));
    }
    Ok(out)
}
pub fn mysql_value(a: &ArrayRef, row: usize, time_zone: &str) -> Result<Value> {
    if a.is_null(row) {
        return Ok(Value::NULL);
    }
    macro_rules! v {
        ($ty:ty) => {
            a.as_any().downcast_ref::<$ty>().unwrap().value(row)
        };
    }
    Ok(match a.data_type() {
        DataType::Boolean => Value::Int(i64::from(v!(BooleanArray))),
        DataType::Int8 => Value::Int(v!(Int8Array) as i64),
        DataType::Int16 => Value::Int(v!(Int16Array) as i64),
        DataType::Int32 => Value::Int(v!(Int32Array) as i64),
        DataType::Int64 => Value::Int(v!(Int64Array)),
        DataType::UInt8 => Value::UInt(v!(UInt8Array) as u64),
        DataType::UInt16 => Value::UInt(v!(UInt16Array) as u64),
        DataType::UInt32 => Value::UInt(v!(UInt32Array) as u64),
        DataType::UInt64 => Value::UInt(v!(UInt64Array)),
        DataType::Float32 => Value::Float(v!(Float32Array)),
        DataType::Float64 => Value::Double(v!(Float64Array)),
        DataType::Utf8 => Value::Bytes(v!(StringArray).as_bytes().to_vec()),
        DataType::Binary => Value::Bytes(v!(BinaryArray).to_vec()),
        DataType::Decimal128(_, s) => {
            Value::Bytes(decimal_string(v!(Decimal128Array), *s).into_bytes())
        }
        DataType::Decimal256(_, s) => {
            Value::Bytes(decimal_string(v!(Decimal256Array), *s).into_bytes())
        }
        DataType::Date32 => {
            let d = NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .checked_add_signed(chrono::Duration::days(v!(Date32Array) as i64))
                .ok_or_else(|| type_error("Date overflow"))?;
            Value::Date(
                d.year()
                    .try_into()
                    .map_err(|_| type_error("Date year overflow"))?,
                d.month() as u8,
                d.day() as u8,
                0,
                0,
                0,
                0,
            )
        }
        DataType::Timestamp(unit, tz) => {
            let micros = match unit {
                TimeUnit::Second => v!(TimestampSecondArray).checked_mul(1_000_000),
                TimeUnit::Millisecond => v!(TimestampMillisecondArray).checked_mul(1000),
                TimeUnit::Microsecond => Some(v!(TimestampMicrosecondArray)),
                TimeUnit::Nanosecond => {
                    let x = v!(TimestampNanosecondArray);
                    if x % 1000 != 0 {
                        return Err(type_error("Nanosecond precision loss"));
                    }
                    Some(x / 1000)
                }
            }
            .ok_or_else(|| type_error("Timestamp overflow"))?;
            let micros = micros
                .checked_add(if tz.is_some() {
                    offset_seconds(time_zone)? as i64 * 1_000_000
                } else {
                    0
                })
                .ok_or_else(|| type_error("Timestamp overflow"))?;
            let dt = chrono::DateTime::from_timestamp_micros(micros)
                .ok_or_else(|| type_error("Timestamp overflow"))?
                .naive_utc();
            datetime_value(dt)?
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            let value = v!(DurationMicrosecondArray);
            let x = value.unsigned_abs();
            let sec = x / 1_000_000;
            if sec > 838 * 3600 + 59 * 60 + 59 {
                return Err(type_error("MySQL TIME outside ±838:59:59"));
            }
            Value::Time(
                value < 0,
                (sec / 86400) as u32,
                ((sec / 3600) % 24) as u8,
                ((sec / 60) % 60) as u8,
                (sec % 60) as u8,
                (x % 1_000_000) as u32,
            )
        }
        other => return Err(type_error(format!("Unsupported MySQL bind type {other:?}"))),
    })
}
fn datetime_value(dt: NaiveDateTime) -> Result<Value> {
    Ok(Value::Date(
        dt.year()
            .try_into()
            .map_err(|_| type_error("Date year overflow"))?,
        dt.month() as u8,
        dt.day() as u8,
        dt.hour() as u8,
        dt.minute() as u8,
        dt.second() as u8,
        dt.and_utc().timestamp_subsec_micros(),
    ))
}
pub fn logical_row_bytes(batch: &RecordBatch, row: usize) -> usize {
    batch
        .columns()
        .iter()
        .map(|a| match a.data_type() {
            DataType::Utf8 => {
                a.as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row)
                    .len()
                    + 16
            }
            DataType::Binary => {
                a.as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(row)
                    .len()
                    + 16
            }
            DataType::LargeUtf8 => {
                a.as_any()
                    .downcast_ref::<LargeStringArray>()
                    .unwrap()
                    .value(row)
                    .len()
                    + 16
            }
            DataType::LargeBinary => {
                a.as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .unwrap()
                    .value(row)
                    .len()
                    + 16
            }
            _ => 32,
        })
        .sum()
}
pub fn check_declared(
    schema: &Schema,
    declared: &std::collections::BTreeMap<String, String>,
) -> Result<()> {
    for (name, typ) in declared {
        let field = schema
            .field_with_name(name)
            .map_err(|_| Error::new("SCHEMA", format!("Declared column {name} not found")))?;
        if format!("{:?}", field.data_type()) != *typ {
            return Err(type_error(format!(
                "{name}: expected {typ}, received {:?}",
                field.data_type()
            )));
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_decimal() {
        let s = "123456789012345678901234567890123456789012345678901234567890.12345";
        assert_eq!(decimal_string(decimal_parse(s, 65, 5).unwrap(), 5), s);
        assert!(decimal_parse("1.001", 3, 2).is_err());
        assert_eq!(
            decimal_string(decimal_parse("-0.0100", 4, 2).unwrap(), 2),
            "-0.01"
        );
    }
    #[test]
    fn casts_reject_loss() {
        let a: ArrayRef = Arc::new(UInt64Array::from(vec![u64::MAX]));
        assert!(checked_cast(&a, &DataType::Int64, "+00:00").is_err());
        let a: ArrayRef = Arc::new(Int64Array::from(vec![9_007_199_254_740_993]));
        assert!(checked_cast(&a, &DataType::Float64, "+00:00").is_err());
    }
    #[test]
    fn negative_duration() {
        let a: ArrayRef = Arc::new(DurationMicrosecondArray::from(vec![-90_000_000_001]));
        assert_eq!(
            mysql_value(&a, 0, "+00:00").unwrap(),
            Value::Time(true, 1, 1, 0, 0, 1)
        );
    }
    #[test]
    fn timestamp_semantics() {
        let a: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![0]).with_timezone("UTC"));
        let b = checked_cast(
            &a,
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            "+08:00",
        )
        .unwrap();
        assert_eq!(
            b.as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(0),
            28_800_000_000
        );
    }
}

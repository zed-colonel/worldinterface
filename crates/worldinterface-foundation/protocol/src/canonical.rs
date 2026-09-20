use crate::{ContentHash, Record, Result};
use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};
use serde_json::Value;
use std::fmt;

pub const CONTRACT: &str = "WI-FABRIC-2";
pub const CANONICAL_PROFILE: &str = "wi-json-1";
pub const MAX_RECORD_BYTES: usize = 65536;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    pub contract: String,
    pub canonical_profile: String,
    pub schema: u32,
    pub record: Record,
}
impl Envelope {
    pub fn new(record: Record) -> Self {
        Self {
            contract: CONTRACT.into(),
            canonical_profile: CANONICAL_PROFILE.into(),
            schema: 1,
            record,
        }
    }
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let value = strict_json(bytes)?;
        let envelope: Self = serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
        // Prevent alternate scalar encodings, explicit absent/null fields and defaults.
        if serde_json::to_value(&envelope).map_err(|e| e.to_string())? != value {
            return Err("non-schema representation".into());
        }
        envelope.canonical_bytes()?;
        Ok(envelope)
    }
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        if self.contract != CONTRACT
            || self.canonical_profile != CANONICAL_PROFILE
            || self.schema != 1
        {
            return Err("unsupported envelope".into());
        }
        self.record.validate()?;
        canonical_data(self)
    }
    pub fn digest(&self) -> Result<ContentHash> {
        let mut bytes = b"WI-FABRIC-2\0wi-json-1\0record\0".to_vec();
        bytes.extend(self.canonical_bytes()?);
        Ok(ContentHash::of(&bytes))
    }
}

/// Deterministic encoding for local manifests/events; not a public record hash API.
pub fn canonical_data<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    canonical_bounded(value, MAX_RECORD_BYTES)
}

/// Internal collection encoding, bounded separately from the unchanged wire record bound.
pub fn canonical_collection<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    canonical_bounded(value, 16 * 1024 * 1024)
}
fn canonical_bounded<T: Serialize>(value: &T, limit: usize) -> Result<Vec<u8>> {
    let v = serde_json::to_value(value).map_err(|e| e.to_string())?;
    validate_value(&v)?;
    // Sort explicitly: downstream serde_json feature unification must not change bytes.
    fn encode(v: &Value, out: &mut Vec<u8>) {
        match v {
            Value::Object(map) => {
                out.push(b'{');
                let mut keys: Vec<_> = map.keys().collect();
                keys.sort();
                for (i, key) in keys.into_iter().enumerate() {
                    if i > 0 {
                        out.push(b',');
                    }
                    out.extend(serde_json::to_vec(key).expect("string serialization"));
                    out.push(b':');
                    encode(&map[key], out);
                }
                out.push(b'}');
            }
            Value::Array(values) => {
                out.push(b'[');
                for (i, value) in values.iter().enumerate() {
                    if i > 0 {
                        out.push(b',');
                    }
                    encode(value, out);
                }
                out.push(b']');
            }
            _ => out.extend(serde_json::to_vec(v).expect("validated JSON scalar")),
        }
    }
    let mut bytes = Vec::new();
    encode(&v, &mut bytes);
    if bytes.len() > limit {
        return Err("record exceeds size bound".into());
    }
    Ok(bytes)
}
fn validate_value(v: &Value) -> Result<()> {
    match v {
        Value::Null => return Err("null is not in this schema".into()),
        Value::Number(n) => {
            if !n
                .as_i64()
                .is_some_and(|n| (-9_007_199_254_740_991..=9_007_199_254_740_991).contains(&n))
            {
                return Err("integer outside canonical bound or floating point".into());
            }
        }
        Value::Array(a) => {
            for v in a {
                validate_value(v)?;
            }
        }
        Value::Object(o) => {
            for v in o.values() {
                validate_value(v)?;
            }
        }
        _ => {}
    }
    Ok(())
}
struct Unique(Value);
impl<'de> Deserialize<'de> for Unique {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Unique;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("JSON with unique keys")
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<Unique, E> {
                Ok(Unique(v.into()))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<Unique, E> {
                Ok(Unique(v.into()))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<Unique, E> {
                Ok(Unique(v.into()))
            }
            fn visit_f64<E: de::Error>(self, _: f64) -> std::result::Result<Unique, E> {
                Err(E::custom("floating point prohibited"))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Unique, E> {
                Ok(Unique(v.into()))
            }
            fn visit_unit<E: de::Error>(self) -> std::result::Result<Unique, E> {
                Err(E::custom("null prohibited"))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Unique, A::Error> {
                let mut out = vec![];
                while let Some(Unique(v)) = a.next_element()? {
                    out.push(v);
                }
                Ok(Unique(Value::Array(out)))
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Unique, A::Error> {
                let mut out = serde_json::Map::new();
                while let Some((k, Unique(v))) = a.next_entry::<String, Unique>()? {
                    if out.insert(k, v).is_some() {
                        return Err(de::Error::custom("duplicate key"));
                    }
                }
                Ok(Unique(Value::Object(out)))
            }
        }
        d.deserialize_any(V)
    }
}
pub fn strict_json(bytes: &[u8]) -> Result<Value> {
    strict_bounded(bytes, MAX_RECORD_BYTES)
}
/// Decode only trusted internal collection envelopes, never transport or public records.
pub fn strict_collection(bytes: &[u8]) -> Result<Value> {
    strict_bounded(bytes, 16 * 1024 * 1024)
}
fn strict_bounded(bytes: &[u8], limit: usize) -> Result<Value> {
    if bytes.len() > limit {
        return Err("record exceeds size bound".into());
    }
    let Unique(v) = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    validate_value(&v)?;
    Ok(v)
}

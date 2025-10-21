use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serializer, ser::SerializeSeq};
use wnfs_common::Cid;

pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<Cid>, D::Error>
where
    D: Deserializer<'de>,
{
    let strings: Vec<String> = Vec::<String>::deserialize(deserializer)?;
    let cids = strings
        .iter()
        .map(|s| Cid::from_str(s))
        .collect::<Result<Vec<Cid>, _>>()
        .map_err(serde::de::Error::custom)?;
    Ok(cids)
}

pub(crate) fn serialize<S>(cids: &Vec<Cid>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut seq = serializer.serialize_seq(Some(cids.len()))?;
    for cid in cids {
        seq.serialize_element(&cid.to_string())?;
    }
    seq.end()
}

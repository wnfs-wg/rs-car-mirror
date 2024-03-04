use serde::{de::Visitor, Deserializer, Serializer};

pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    struct BytesOrStringVisitor;

    impl Visitor<'_> for BytesOrStringVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("bytes, byte buf or string")
        }

        fn visit_borrowed_bytes<E>(self, v: &'_ [u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(v.to_vec())
        }

        fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(v)
        }

        fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(v.to_vec())
        }

        fn visit_borrowed_str<E>(self, v: &'_ str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            data_encoding::BASE64URL_NOPAD
                .decode(v.as_bytes())
                .map_err(serde::de::Error::custom)
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            data_encoding::BASE64URL_NOPAD
                .decode(v.as_bytes())
                .map_err(serde::de::Error::custom)
        }

        fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            data_encoding::BASE64URL_NOPAD
                .decode(v.as_bytes())
                .map_err(serde::de::Error::custom)
        }
    }

    deserializer.deserialize_any(BytesOrStringVisitor)
}

pub(crate) fn serialize<S>(bloom_bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if serializer.is_human_readable() {
        serializer.serialize_str(
            data_encoding::BASE64URL_NOPAD
                .encode(bloom_bytes.as_ref())
                .as_ref(),
        )
    } else {
        serializer.serialize_bytes(bloom_bytes.as_ref())
    }
}

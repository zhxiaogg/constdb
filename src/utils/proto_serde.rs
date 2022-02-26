use serde::Deserialize;
use std::fmt::Formatter;
use std::marker::PhantomData;

use protobuf::EnumOrUnknown;
use protobuf::{Enum, MessageField};

#[allow(dead_code)]
pub fn serialize_message_field<M: serde::Serialize, S: serde::Serializer>(
    e: &MessageField<M>,
    s: S,
) -> Result<S::Ok, S::Error> {
    match e {
        MessageField(Some(m)) => s.serialize_some(&m),
        _ => s.serialize_none(),
    }
}

#[allow(dead_code)]
pub fn deserialize_message_field<
    'de,
    M: 'de + serde::Deserialize<'de>,
    D: serde::Deserializer<'de>,
>(
    d: D,
) -> Result<MessageField<M>, D::Error> {
    struct DeserializeMessageFieldVisitor<'de, M: serde::Deserialize<'de>>(PhantomData<&'de M>);

    impl<'de, M: serde::Deserialize<'de>> serde::de::Visitor<'de>
        for DeserializeMessageFieldVisitor<'de, M>
    {
        type Value = MessageField<M>;

        fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
            write!(formatter, "a message object or null")
        }

        fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            Deserialize::deserialize(deserializer).map(|v| MessageField::some(v))
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(MessageField::none())
        }
    }

    d.deserialize_newtype_struct("", DeserializeMessageFieldVisitor::<'de, M>(PhantomData))
}

#[allow(dead_code)]
pub fn serialize_enum_or_unknown<E: Enum, S: serde::Serializer>(
    e: &EnumOrUnknown<E>,
    s: S,
) -> Result<S::Ok, S::Error> {
    match e.enum_value() {
        Ok(v) => s.serialize_str(v.descriptor().name()),
        Err(v) => s.serialize_i32(v),
    }
}

#[allow(dead_code)]
pub fn deserialize_enum_or_unknown<'de, E: Enum, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<EnumOrUnknown<E>, D::Error> {
    struct DeserializeEnumVisitor<E: Enum>(PhantomData<E>);

    impl<'de, E: Enum> serde::de::Visitor<'de> for DeserializeEnumVisitor<E> {
        type Value = EnumOrUnknown<E>;

        fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
            write!(formatter, "a string, an integer or none")
        }

        fn visit_str<R>(self, v: &str) -> Result<Self::Value, R>
        where
            R: serde::de::Error,
        {
            match E::enum_descriptor_static().value_by_name(v) {
                Some(v) => Ok(EnumOrUnknown::from_i32(v.value())),
                None => Err(serde::de::Error::custom(format!(
                    "unknown enum value: {}",
                    v
                ))),
            }
        }

        fn visit_i32<R>(self, v: i32) -> Result<Self::Value, R>
        where
            R: serde::de::Error,
        {
            Ok(EnumOrUnknown::from_i32(v))
        }

        fn visit_unit<R>(self) -> Result<Self::Value, R>
        where
            R: serde::de::Error,
        {
            Ok(EnumOrUnknown::default())
        }
    }

    d.deserialize_any(DeserializeEnumVisitor(PhantomData))
}

use protobuf::descriptor::field_descriptor_proto::Type;
use protobuf::reflect::FieldDescriptor;
use protobuf::reflect::MessageDescriptor;

use protobuf_codegen::Codegen;
use protobuf_codegen::Customize;
use protobuf_codegen::CustomizeCallback;

fn main() {
    struct GenSerde;

    impl CustomizeCallback for GenSerde {
        fn message(&self, _message: &MessageDescriptor) -> Customize {
            Customize::default().before("#[derive(::serde::Serialize, ::serde::Deserialize)]")
        }

        fn field(&self, field: &FieldDescriptor) -> Customize {
            if field.proto().field_type() == Type::TYPE_ENUM {
                // `EnumOrUnknown` is not a part of rust-protobuf, so external serializer is needed.
                Customize::default().before(
                    "#[serde(serialize_with = \"crate::utils::proto_serde::serialize_enum_or_unknown\", deserialize_with = \"crate::utils::proto_serde::deserialize_enum_or_unknown\")]")
            } else if field.proto().field_type() == Type::TYPE_MESSAGE
                && !field.is_repeated_or_map()
            {
                Customize::default().before(
                    "#[serde(serialize_with = \"crate::utils::proto_serde::serialize_message_field\", deserialize_with = \"crate::utils::proto_serde::deserialize_message_field\")]")
            } else {
                Customize::default()
            }
        }

        fn special_field(&self, _message: &MessageDescriptor, _field: &str) -> Customize {
            Customize::default().before("#[serde(skip)]")
        }
    }

    // Use this in build.rs
    Codegen::new()
        .pure()
        .includes(&["src/protos"])
        // Inputs must reside in some of include paths.
        .input("src/protos/constdb_model.proto")
        .out_dir("src/protos")
        .customize_callback(GenSerde)
        .run_from_script();
}

//! T9: provider response bodies come from the network, so `gateway::parse`
//! must refuse any JSON without panicking, parse a well-formed body to
//! exactly its text and usage, and refuse one whose text is missing.
use gateway::{parse, Completion, GatewayError};
use proptest::prelude::*;
use protocol::{MessageRole, ModelMessage, ModelProvider, Usage};
use serde_json::{json, Map, Value};

const PROVIDERS: [ModelProvider; 4] = [
    ModelProvider::OpenAI,
    ModelProvider::Anthropic,
    ModelProvider::Gemini,
    ModelProvider::SystemOne,
];

/// Object keys: the ones the parsers look for, so generated bodies reach
/// deep into them, and any others.
fn key() -> impl Strategy<Value = String> {
    prop_oneof![
        prop::sample::select(vec![
            "choices",
            "message",
            "role",
            "content",
            "text",
            "candidates",
            "parts",
            "usage",
            "usageMetadata",
            "prompt_tokens",
            "completion_tokens",
            "input_tokens",
            "output_tokens",
            "promptTokenCount",
            "candidatesTokenCount",
        ])
        .prop_map(str::to_string),
        "[a-zA-Z_]{0,10}",
    ]
}

fn any_json() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        any::<u64>().prop_map(Value::from),
        any::<f64>()
            .prop_filter("finite", |number| number.is_finite())
            .prop_map(|number| json!(number)),
        prop_oneof![Just("assistant".to_string()), ".{0,16}"].prop_map(Value::from),
    ];
    leaf.prop_recursive(5, 96, 6, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::vec((key(), inner), 0..6)
                .prop_map(|fields| Value::Object(fields.into_iter().collect::<Map<_, _>>())),
        ]
    })
}

/// A well-formed body of `provider` with `text`, and usage when given.
fn body(provider: ModelProvider, text: &str, usage: Option<(u64, u64)>) -> Value {
    let mut body = match provider {
        ModelProvider::OpenAI | ModelProvider::SystemOne => {
            json!({"choices": [{"message": {"role": "assistant", "content": text}}]})
        }
        ModelProvider::Anthropic => json!({"content": [{"type": "text", "text": text}]}),
        ModelProvider::Gemini => json!({"candidates": [{"content": {"parts": [{"text": text}]}}]}),
    };
    if let Some((input, output)) = usage {
        let (name, counts) = match provider {
            ModelProvider::OpenAI | ModelProvider::SystemOne => (
                "usage",
                json!({"prompt_tokens": input, "completion_tokens": output}),
            ),
            ModelProvider::Anthropic => (
                "usage",
                json!({"input_tokens": input, "output_tokens": output}),
            ),
            ModelProvider::Gemini => (
                "usageMetadata",
                json!({"promptTokenCount": input, "candidatesTokenCount": output}),
            ),
        };
        body.as_object_mut()
            .expect("object")
            .insert(name.to_string(), counts);
    }
    body
}

proptest! {
    #[test]
    fn any_json_is_parsed_or_refused_without_panicking(value in any_json()) {
        for provider in PROVIDERS {
            let _ = parse(provider, &value);
        }
    }

    #[test]
    fn a_well_formed_body_parses_to_its_text_and_usage(
        text in ".{0,64}",
        usage in prop::option::of((any::<u64>(), any::<u64>())),
    ) {
        for provider in PROVIDERS {
            let parsed = parse(provider, &body(provider, &text, usage));
            prop_assert_eq!(
                parsed.map_err(|error| error.to_string()),
                Ok(Completion {
                    message: ModelMessage {
                        role: MessageRole::Assistant,
                        text: text.clone(),
                    },
                    usage: usage.map(|(input_tokens, output_tokens)| Usage {
                        input_tokens,
                        output_tokens,
                    }),
                })
            );
        }
    }

    #[test]
    fn a_body_without_its_text_is_malformed(
        text in ".{0,16}",
        usage in prop::option::of((any::<u64>(), any::<u64>())),
    ) {
        for provider in PROVIDERS {
            let mut stripped = body(provider, &text, usage);
            let text_holder = match provider {
                ModelProvider::OpenAI | ModelProvider::SystemOne => {
                    stripped.pointer_mut("/choices/0/message")
                }
                ModelProvider::Anthropic => stripped.pointer_mut("/content/0"),
                ModelProvider::Gemini => stripped.pointer_mut("/candidates/0/content/parts/0"),
            };
            let field = if matches!(provider, ModelProvider::OpenAI | ModelProvider::SystemOne) {
                "content"
            } else {
                "text"
            };
            text_holder
                .and_then(Value::as_object_mut)
                .expect("the text's object")
                .remove(field);
            let refused = matches!(parse(provider, &stripped), Err(GatewayError::Malformed(_)));
            prop_assert!(refused, "{:?}", provider);
        }
    }
}

// Token counts that are not unsigned integers are no usage, not an error.
#[test]
fn usage_that_is_not_a_count_is_none() {
    let body = json!({
        "content": [{"type": "text", "text": "hi"}],
        "usage": {"input_tokens": -1, "output_tokens": "7"}
    });
    let completion = parse(ModelProvider::Anthropic, &body).expect("parsed");
    assert_eq!(completion.usage, None);
}

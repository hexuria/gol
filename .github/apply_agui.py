from pathlib import Path
import subprocess

def sub(path, old, new):
    file = Path(path)
    text = file.read_text()
    if old not in text:
        raise SystemExit(f"missing pattern in {path}")
    file.write_text(text.replace(old, new, 1))

sub(
    "crates/server/src/http.rs",
    "use crate::store::{AgentManifest, RunStore, StoredRun};\n",
    "use crate::store::{AgentManifest, RunStore, StoredRun};\nuse crate::surface::{ag_ui_events, json_render_spec};\n",
)
sub(
    "crates/server/src/http.rs",
    '        .route("/v1/runs/{id}/events", get(get_events))\n',
    '        .route("/v1/runs/{id}/events", get(get_events))\n        .route("/v1/runs/{id}/ag-ui", get(get_ag_ui))\n        .route("/v1/runs/{id}/ui", get(get_ui))\n',
)
sub(
    "crates/server/src/http.rs",
    "fn run_with_jev(jev_base_url: &str, spec: RunSpec) -> Result<Vec<Event>, RunStartError> {",
    """async fn get_ag_ui(
    State(state): State<AppState>,
    Path(id): Path<RunId>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let store = state.store.clone();
    let stored = tokio::task::spawn_blocking(move || store.run(id))
        .await
        .map_err(|error| ApiError::Decider(error.to_string()))?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(serde_json::Value::Array(ag_ui_events(
        stored.spec.run_id,
        &stored.events,
    ))))
}

async fn get_ui(
    State(state): State<AppState>,
    Path(id): Path<RunId>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let store = state.store.clone();
    let stored = tokio::task::spawn_blocking(move || store.run(id))
        .await
        .map_err(|error| ApiError::Decider(error.to_string()))?
        .ok_or(ApiError::NotFound)?;
    let folded = fold(&stored.spec, &stored.events);
    let outcome = match &folded.harness {
        protocol::HarnessState::Completed { outcome } => outcome.clone(),
        protocol::HarnessState::Failed { message, .. } => message.clone(),
        protocol::HarnessState::Cancelled => \"cancelled\".to_string(),
        _ => \"running\".to_string(),
    };
    Ok(Json(json_render_spec(&stored.spec.input, &outcome)))
}

fn run_with_jev(jev_base_url: &str, spec: RunSpec) -> Result<Vec<Event>, RunStartError> {""",
)
sub(
    "crates/server/tests/http_run.rs",
    """    assert!(events
        .iter()
        .any(|event| matches!(&event.payload, EventPayload::RunCompleted { outcome } if outcome == \"done\")));
}
""",
    """    assert!(events
        .iter()
        .any(|event| matches!(&event.payload, EventPayload::RunCompleted { outcome } if outcome == \"done\")));

    let ag_ui = client
        .get(format!(\"{base}/v1/runs/{}/ag-ui\", created.run_id))
        .send()
        .await
        .expect(\"ag-ui\")
        .error_for_status()
        .expect(\"ag-ui status\")
        .json::<Vec<serde_json::Value>>()
        .await
        .expect(\"ag-ui json\");
    assert_eq!(ag_ui.first().and_then(|event| event.get(\"type\")), Some(&serde_json::json!(\"RUN_STARTED\")));
    assert!(ag_ui.iter().any(|event| event.get(\"type\") == Some(&serde_json::json!(\"TOOL_CALL_RESULT\"))));
    assert_eq!(ag_ui.last().and_then(|event| event.get(\"type\")), Some(&serde_json::json!(\"RUN_FINISHED\")));

    let ui = client
        .get(format!(\"{base}/v1/runs/{}/ui\", created.run_id))
        .send()
        .await
        .expect(\"ui\")
        .error_for_status()
        .expect(\"ui status\")
        .json::<serde_json::Value>()
        .await
        .expect(\"ui json\");
    assert_eq!(ui.get(\"root\").and_then(|value| value.as_str()), Some(\"screen\"));
    assert_eq!(
        ui.pointer(\"/elements/outcome/props/text\").and_then(|value| value.as_str()),
        Some(\"done\")
    );
}
""",
)
expected = {
    "crates/server/src/http.rs": "053fb95551f178e16d7b6f2825a1495be576a6af",
    "crates/server/tests/http_run.rs": "2e7c0bcdf7541b760dcb2afa25663f01a8061047",
}
for path, sha in expected.items():
    actual = subprocess.check_output(["git", "hash-object", path], text=True).strip()
    print(path, actual)
    if actual != sha:
        raise SystemExit(f"hash mismatch {path}")

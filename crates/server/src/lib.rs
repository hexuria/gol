#![forbid(unsafe_code)]
/// Bearer-token authentication; its builder state markers live here.
pub mod auth;
mod deliverer;
mod http;
mod inference;
mod local;
mod models;
mod postgres;
mod queue;
mod scheduler;
mod spawner;
mod store;
mod stores;
mod stream;
mod surface;
/// Webhook requests (Phase 4.3).
pub mod tools;
mod triggers;
pub mod webhook;
mod worker;

pub use auth::{
    auth_from_env, AuthError, Authenticator, LocalDevAuthenticator, OidcConfig, OidcConfigBuilder,
    OidcVerifier, Principal, LOCAL_DEV_TOKEN,
};
pub use deliverer::{OwnedDeliverer, OwnedDelivererBuilder};
pub use http::{
    router, router_with_gateway, router_with_memory, router_with_queue, router_with_sandbox,
    router_with_webhooks,
};
pub use inference::{
    accept_subscription_completion, box_attempt_name, box_container_name, box_run_of,
    box_workspace_volume, computer_plan, ensure_fixture_proxy, fail_turn, open_turn, queued_events,
    run_failed_event, sandbox_from_env, workspace_run_of, DockerSandbox, GatewayCall,
    GatewayPoster, HttpGatewayPoster, MemorySandbox, SandboxError, SandboxHost, TurnError,
};
pub use local::LocalEchoFactory;
pub use models::{GatewayModel, ModelsConfig};
pub use postgres::{PoolOptions, PostgresStore};
pub use queue::{QueueTiming, RedisRunQueue};
pub use scheduler::{fire_due_trigger, next_tick, schedule_due, schedule_forever, MISSED_AFTER_MS};
pub use spawner::OwnedSpawner;
pub use store::{
    is_terminal, thread_of, AgentManifest, Append, InMemoryStore, MessageStore, Missed,
    OutboxEntry, OutboxStore, PutAgent, PutMessage, PutRun, RunStore, StopScope, StopStore,
    StoredAgent, StoredArtifact, StoredMessage, StoredRun, StoredTrigger, ThreadStore,
    ThreadSummary, TriggerId, TriggerKind, TriggerStore,
};
pub use stores::{stores_from_env, Stores};
pub use surface::{ag_ui_events, json_render_spec};
pub use triggers::{
    fire_trigger, fire_trigger_at, fire_webhook, trigger_thread, webhook_secret, FireError, Fired,
    Hooked, MAX_TRIGGERS, MIN_WEBHOOK_KEY_BYTES, TRIGGER_KEY,
};
pub use worker::{
    queue_from_env, reap_forever, start_queue, start_sandbox_sweep, sweep, sweep_asks,
    sweep_sandboxes, sweep_sandboxes_forever, Claim, Done, Executed, Open, Prepared, QueueSettings,
    Worker, WorkerBuilder,
};

/// Returned by every `RunStore` method.
pub use harness::StoreError;

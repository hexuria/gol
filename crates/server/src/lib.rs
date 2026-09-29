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
mod spawner;
mod store;
mod stores;
mod stream;
mod surface;
mod worker;

pub use auth::{
    auth_from_env, AuthError, Authenticator, LocalDevAuthenticator, OidcConfig, OidcConfigBuilder,
    OidcVerifier, Principal, LOCAL_DEV_TOKEN,
};
pub use deliverer::{OwnedDeliverer, OwnedDelivererBuilder};
pub use http::{
    router, router_with_gateway, router_with_memory, router_with_queue, router_with_sandbox,
};
pub use inference::{
    accept_subscription_completion, box_container_name, box_workspace_volume, computer_plan,
    ensure_fixture_proxy, fail_turn, open_turn, queued_events, run_failed_event, sandbox_from_env,
    DockerSandbox, GatewayCall, GatewayPoster, HttpGatewayPoster, MemorySandbox, SandboxError,
    SandboxHost, TurnError,
};
pub use local::LocalEchoFactory;
pub use models::{GatewayModel, ModelsConfig};
pub use postgres::{PoolOptions, PostgresStore};
pub use queue::{QueueTiming, RedisRunQueue};
pub use spawner::OwnedSpawner;
pub use store::{
    is_terminal, thread_of, AgentManifest, Append, InMemoryStore, MessageStore, OutboxEntry,
    OutboxStore, PutAgent, PutMessage, PutRun, RunStore, StoredAgent, StoredArtifact,
    StoredMessage, StoredRun, ThreadStore, ThreadSummary,
};
pub use stores::{stores_from_env, Stores};
pub use surface::{ag_ui_events, json_render_spec};
pub use worker::{
    queue_from_env, reap_forever, start_queue, sweep, sweep_asks, Claim, Done, Executed, Open,
    Prepared, QueueSettings, Worker, WorkerBuilder,
};

/// Returned by every `RunStore` method.
pub use harness::StoreError;

//! Threads and the board (Phase 3.3, decisions 47A-50A): a thread is a
//! coordinator run of an agent the caller picks, named by its session id;
//! its Complete is the quick answer, its delegations are the tasks, and a
//! follow-up is a new coordinator run in the same session. The board lists
//! a thread's runs as cards, with their state from the fold of each log. On
//! both stores; needs Postgres and Redis, as `pg_redis.rs` does. The queued
//! tests each use a Redis database of their own.
mod common;

use std::sync::Arc;

use common::queued::{blocking, fresh_user, jev, serve, stores, Store, POSTGRES, POSTGRES_URL};
use protocol::{AgentId, RunId};
use serde_json::{json, Value};
use server::{PostgresStore, RunStore};

/// The board's cards: (run, state, parent).
fn cards(board: &Value) -> Vec<(String, String, Option<String>)> {
    board["cards"]
        .as_array()
        .expect("cards")
        .iter()
        .map(|card| {
            (
                card["run_id"].as_str().expect("run").to_string(),
                card["state"].as_str().expect("state").to_string(),
                card["parent"].as_str().map(str::to_string),
            )
        })
        .collect()
}

// Case 1: the coordinator hands two asks to other agents and answers the
// question itself. Its answer is stored while both tasks are still queued,
// and the board shows the coordinator with its two tasks. The tasks run
// next, one of them delegating in turn: the board shows every run of the
// thread at any depth, and filters by state.
#[tokio::test(flavor = "multi_thread")]
async fn two_asks_and_a_question_answer_at_once() {
    for store in stores() {
        let jev = jev(&[
            "delegate:writer",
            "delegate:researcher",
            "complete",
            // The writer, then the researcher, which delegates in turn,
            // then its own task.
            "complete",
            "delegate:writer",
            "complete",
            "complete",
        ])
        .await;
        let server = serve(store, &jev, 7).await;
        let user = fresh_user();
        let coordinator = server
            .agent(&user, "coordinator", &["agent.delegate"])
            .await;
        server.agent(&user, "writer", &[]).await;
        let researcher_agent = server.agent(&user, "researcher", &["agent.delegate"]).await;
        let (thread, first) = server.start(&user, coordinator, "plan a trip").await;

        assert_eq!(
            server.work().await.map(|run| run.to_string()),
            Some(first.clone())
        );
        let (status, board) = server
            .get(&format!("/v1/threads/{thread}/board"), &user)
            .await;
        assert_eq!(status, 200, "{board}");
        let now = cards(&board);
        let board_before = board.clone();
        assert_eq!(now.len(), 3, "{board}");
        assert_eq!(now[0], (first.clone(), "completed".to_string(), None));
        assert_eq!(board["cards"][0]["outcome"], "done");
        assert_eq!(
            board["cards"][0]["children"]
                .as_array()
                .expect("children")
                .len(),
            2
        );
        for task in &now[1..] {
            assert_eq!(task.1, "queued", "{board}");
            assert_eq!(task.2.as_deref(), Some(first.as_str()));
        }

        // The writer, the researcher, and the researcher's own task.
        for _ in 0..3 {
            assert!(server.work().await.is_some());
        }
        let (_, board) = server
            .get(&format!("/v1/threads/{thread}/board"), &user)
            .await;
        let all = cards(&board);
        assert_eq!(all.len(), 4, "{board}");
        assert!(all.iter().all(|card| card.1 == "completed"), "{board}");
        // The researcher's card by its agent: tasks stored in one
        // millisecond have no set order between them.
        let researcher = board_before["cards"]
            .as_array()
            .expect("cards")
            .iter()
            .find(|card| card["agent_id"] == researcher_agent.to_string())
            .expect("the researcher's card")["run_id"]
            .as_str()
            .expect("run")
            .to_string();
        assert!(all
            .iter()
            .any(|card| card.2.as_deref() == Some(researcher.as_str())));
        assert_eq!(board["truncated"], false);
        let (_, queued) = server
            .get(&format!("/v1/threads/{thread}/board?state=queued"), &user)
            .await;
        assert_eq!(cards(&queued), Vec::new());
        let (_, done) = server
            .get(
                &format!("/v1/threads/{thread}/board?state=completed"),
                &user,
            )
            .await;
        assert_eq!(cards(&done).len(), 4);
    }
}

// The board of a thread in mixed states: ?state= picks its cards, and the
// board pages them; a card says when it started, or null while queued. The
// thread list takes no state.
#[tokio::test(flavor = "multi_thread")]
async fn the_board_lists_a_threads_tasks_by_state() {
    for store in stores() {
        let jev = jev(&[
            "delegate:writer",
            "delegate:researcher",
            "complete",
            "complete",
        ])
        .await;
        let server = serve(store, &jev, 11).await;
        let user = fresh_user();
        let coordinator = server
            .agent(&user, "coordinator", &["agent.delegate"])
            .await;
        server.agent(&user, "writer", &[]).await;
        server.agent(&user, "researcher", &[]).await;
        let (thread, first) = server.start(&user, coordinator, "plan a trip").await;
        // The coordinator, then one of its two tasks; the other stays queued.
        assert!(server.work().await.is_some());
        assert!(server.work().await.is_some());
        let board = |query: &str| {
            let path = format!("/v1/threads/{thread}/board{query}");
            let server = &server;
            let user = user.clone();
            async move { server.get(&path, &user).await }
        };
        let (status, all) = board("").await;
        assert_eq!(status, 200, "{all}");
        let states: Vec<String> = cards(&all).into_iter().map(|card| card.1).collect();
        assert_eq!(states.len(), 3, "{all}");
        assert_eq!(states[0], "completed");
        assert_eq!(
            states.iter().filter(|state| *state == "completed").count(),
            2,
            "{all}"
        );
        let (_, queued) = board("?state=queued").await;
        let queued = cards(&queued);
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].1, "queued");
        assert_eq!(queued[0].2.as_deref(), Some(first.as_str()));
        let (_, completed) = board("?state=completed").await;
        assert_eq!(cards(&completed).len(), 2);
        // A started run says when; a queued one has not started.
        let started = |board: &Value, run: &str| {
            board["cards"]
                .as_array()
                .expect("cards")
                .iter()
                .find(|card| card["run_id"] == run)
                .expect("the card")["started_at"]
                .clone()
        };
        assert!(started(&all, &first).is_number(), "{all}");
        assert_eq!(started(&all, &queued[0].0), Value::Null);
        // Pages of the board, oldest first.
        let (_, page) = board("?limit=1").await;
        assert_eq!(cards(&page).len(), 1);
        assert_eq!(cards(&page)[0].0, first);
        let (_, next) = board("?after=1&limit=1").await;
        assert_eq!(cards(&next).len(), 1);
        assert_eq!(cards(&next)[0].0, cards(&all)[1].0);
        let (_, past) = board("?after=3").await;
        assert_eq!(cards(&past), Vec::new());
        let (_, second) = board("?state=completed&after=1&limit=1").await;
        assert_eq!(cards(&second).len(), 1);
        assert_eq!(cards(&second)[0].1, "completed");
        assert_ne!(cards(&second)[0].0, first);
        assert_eq!(board("?state=sleeping").await.0, 400);
        assert_eq!(server.get("/v1/threads?state=queued", &user).await.0, 400);
    }
}

// A follow-up is a new coordinator run of the same agent in the same thread;
// the thread lists once, with its runs counted.
#[tokio::test(flavor = "multi_thread")]
async fn a_follow_up_is_a_new_run_in_the_same_thread() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let server = serve(store, &jev, 8).await;
        let user = fresh_user();
        let coordinator = server.agent(&user, "coordinator", &[]).await;
        let (thread, first) = server.start(&user, coordinator, "hello").await;
        assert!(server.work().await.is_some());
        let (status, body) = server
            .post(
                &format!("/v1/threads/{thread}/messages"),
                &user,
                json!({"input": "and then?"}),
            )
            .await;
        assert_eq!(status, 200, "{body}");
        let second = body["run"]["run_id"].as_str().expect("run").to_string();
        assert_ne!(second, first);
        assert!(server.work().await.is_some());
        let (_, board) = server
            .get(&format!("/v1/threads/{thread}/board"), &user)
            .await;
        assert_eq!(
            cards(&board),
            [
                (first, "completed".to_string(), None),
                (second, "completed".to_string(), None)
            ]
        );
        assert_eq!(board["cards"][1]["agent_id"], coordinator.to_string());
        let (status, threads) = server.get("/v1/threads", &user).await;
        assert_eq!(status, 200);
        assert_eq!(
            threads["threads"],
            json!([{
                "thread_id": thread,
                "agent_id": coordinator,
                "runs": 2,
                "started_at": threads["threads"][0]["started_at"],
            }])
        );
        // A newer thread lists first; pages take the rest.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let (newer, _) = server.start(&user, coordinator, "another").await;
        let (_, threads) = server.get("/v1/threads", &user).await;
        let listed: Vec<&str> = threads["threads"]
            .as_array()
            .expect("threads")
            .iter()
            .map(|thread| thread["thread_id"].as_str().expect("id"))
            .collect();
        assert_eq!(listed, [newer.as_str(), thread.as_str()]);
        let (_, rest) = server.get("/v1/threads?after=1&limit=1", &user).await;
        assert_eq!(rest["threads"][0]["thread_id"], thread);
        assert_eq!(server.get("/v1/threads?page=2", &user).await.0, 400);
        let (status, missing) = server.get("/v1/threads/no-such-thread/board", &user).await;
        assert_eq!(status, 404);
        assert_eq!(missing["error"], "thread not found");
        // The agent's owner moves it to a new version: a follow-up runs the
        // version stored now, not the thread's first.
        assert!(
            server
                .put_version(&user, coordinator, "coordinator", "2")
                .await
        );
        let (status, body) = server
            .post(
                &format!("/v1/threads/{thread}/messages"),
                &user,
                json!({"input": "still there?"}),
            )
            .await;
        assert_eq!(status, 200, "{body}");
        let third: RunId = body["run"]["run_id"]
            .as_str()
            .expect("run")
            .parse()
            .expect("an id");
        let store = server.store.clone();
        let version = blocking(move || {
            store
                .run(third)
                .expect("read")
                .expect("stored")
                .spec
                .agent_version
        })
        .await;
        assert_eq!(version, "2");
        assert!(server.work().await.is_some());
    }
}

// Another principal's thread is not found, and is not listed.
#[tokio::test(flavor = "multi_thread")]
async fn another_principals_thread_is_not_found() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let server = serve(store, &jev, 9).await;
        let (alice, bob) = (fresh_user(), fresh_user());
        let coordinator = server.agent(&alice, "coordinator", &[]).await;
        let (thread, _) = server.start(&alice, coordinator, "hello").await;
        let board = format!("/v1/threads/{thread}/board");
        assert_eq!(server.get(&board, &bob).await.0, 404);
        let follow_up = format!("/v1/threads/{thread}/messages");
        assert_eq!(
            server
                .post(&follow_up, &bob, json!({"input": "mine now"}))
                .await
                .0,
            404
        );
        let (status, threads) = server.get("/v1/threads", &bob).await;
        assert_eq!(status, 200);
        assert_eq!(threads["threads"], json!([]));
        // A session the caller names itself is refused: a thread's session
        // is the server's to choose.
        let (status, _) = server
            .post(
                "/v1/threads",
                &alice,
                json!({
                    "agent_id": coordinator, "agent_version": "1", "input": "x",
                    "placement": "Local",
                    "work_model": {"provider": "OpenAI", "model_name": "gpt-test",
                        "credential": "PlatformGateway"},
                    "metadata": {"session_id": thread},
                }),
            )
            .await;
        assert_eq!(status, 400);
    }
}

// Decision 49A: a database whose runs table predates the thread columns gets
// them at connect, filled from each run's spec.
#[test]
fn an_old_database_gets_its_columns_backfilled() {
    let mut admin = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("connect");
    let schema = format!("p33_old_{}", std::process::id());
    let owner = protocol::Owner::new(common::ISSUER, fresh_user(), "tenant-1");
    let agent = AgentId::new();
    let run = RunId::new();
    let spec = json!({
        "run_id": run, "owner": owner, "agent_id": agent, "agent_version": "1",
        "input": "old", "placement": "Local",
        "work_model": {"provider": "OpenAI", "model_name": "gpt-test",
            "credential": "PlatformGateway"},
        "capabilities": [], "limits": {"max_steps": 4, "max_model_calls": 1},
        "metadata": {"session_id": "old-thread"},
    });
    admin
        .batch_execute(&format!(
            "drop schema if exists {schema} cascade;
             create schema {schema};
             create table {schema}.runs (id uuid primary key, spec jsonb not null);"
        ))
        .expect("old schema");
    admin
        .execute(
            &format!("insert into {schema}.runs (id, spec) values ($1, $2)"),
            &[&run.as_uuid(), &spec],
        )
        .expect("old run");
    // A task of that run, stored before the columns with its first event.
    let task = RunId::new();
    let mut task_spec = spec.clone();
    task_spec["run_id"] = json!(task);
    task_spec["lineage"] = json!({"parent": run, "root": run, "hop": 1});
    admin
        .batch_execute(&format!(
            "create table {schema}.run_events (
                 run_id uuid not null references {schema}.runs (id),
                 seq bigint not null, body jsonb not null, terminal boolean not null,
                 primary key (run_id, seq));"
        ))
        .expect("old events");
    admin
        .execute(
            &format!("insert into {schema}.runs (id, spec) values ($1, $2)"),
            &[&task.as_uuid(), &task_spec],
        )
        .expect("old task");
    let created = protocol::Event::record(
        protocol::EventSource::new(
            task,
            agent,
            "1",
            protocol::Actor::System,
            protocol::Timestamp::unix_millis(1_234),
        ),
        protocol::EventPayload::RunCreated,
    );
    admin
        .execute(
            &format!("insert into {schema}.run_events values ($1, 1, $2, false)"),
            &[&task.as_uuid(), &serde_json::to_value(&created).unwrap()],
        )
        .expect("old event");
    let url = format!("{POSTGRES_URL}?options=-csearch_path%3D{schema}");
    let listed = PostgresStore::connect(&url).and_then(|store| {
        let threads = store.threads().expect("threads");
        let listed = threads.threads_of(&owner, 0, 50)?;
        let runs = threads.runs_of_thread(&owner, "old-thread", 50)?;
        Ok((listed, runs))
    });
    admin
        .batch_execute(&format!("drop schema {schema} cascade"))
        .expect("drop");
    let (listed, runs) = listed.expect("connect and list");
    // The task sorts by its first event's time; the run without one gets
    // the migration's, after it.
    assert_eq!(
        runs.iter().map(|run| run.spec.run_id).collect::<Vec<_>>(),
        [task, run]
    );
    assert_eq!(runs[0].spec.lineage.parent, Some(run));
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(listed[0].thread_id, "old-thread");
    assert_eq!(listed[0].agent_id, agent);
    assert_eq!(listed[0].runs, 2);
}

// A run an older server stored, without the thread columns (a rolling
// deploy), still takes appends; the next connect fills its columns, and it
// is listed.
#[test]
fn a_run_stored_by_an_older_server_still_takes_appends() {
    let store = POSTGRES
        .get_or_init(|| Arc::new(PostgresStore::connect(POSTGRES_URL).expect("connect")) as Store)
        .clone();
    let owner = protocol::Owner::new(common::ISSUER, fresh_user(), "tenant-1");
    let run = RunId::new();
    let spec = json!({
        "run_id": run, "owner": owner, "agent_id": AgentId::new(), "agent_version": "1",
        "input": "old", "placement": "Local",
        "work_model": {"provider": "OpenAI", "model_name": "gpt-test",
            "credential": "PlatformGateway"},
        "capabilities": [], "limits": {"max_steps": 4, "max_model_calls": 1},
        "metadata": {"session_id": "rolling"},
    });
    let mut admin = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("admin");
    admin
        .execute(
            "insert into runs (id, spec) values ($1, $2)",
            &[&run.as_uuid(), &spec],
        )
        .expect("an older server's insert");
    let spec: protocol::RunSpec = serde_json::from_value(spec).expect("spec");
    let message = protocol::Event::record(
        protocol::EventSource::for_spec(&spec, protocol::Actor::System, protocol::Timestamp::now()),
        protocol::EventPayload::UserMessage {
            text: "hi".to_string(),
        },
    );
    assert_eq!(
        store.append_events(run, vec![message]),
        Ok(server::Append::Appended)
    );
    let fresh = PostgresStore::connect(POSTGRES_URL).expect("reconnect");
    let listed = fresh
        .threads()
        .expect("threads")
        .threads_of(&owner, 0, 50)
        .expect("list");
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(listed[0].thread_id, "rolling");
}

// A run with an empty session id is not a thread, and a card lists a child
// once even when its log names it twice (a resumed run).
#[tokio::test(flavor = "multi_thread")]
async fn an_empty_session_is_no_thread_and_children_list_once() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let server = serve(store.clone(), &jev, 10).await;
        let user = fresh_user();
        let owner = protocol::Owner::new(common::ISSUER, user.clone(), "tenant-1");
        let spec_with = |session: &str| {
            protocol::RunSpec::builder()
                .owner(owner.clone())
                .agent(AgentId::new(), "1")
                .input("x")
                .placement(protocol::ExecutionPlacement::Local)
                .work_model(protocol::WorkModel {
                    provider: protocol::ModelProvider::OpenAI,
                    model_name: "gpt-test".to_string(),
                    credential: protocol::CredentialSource::PlatformGateway,
                })
                .metadata([(protocol::SESSION_ID.to_string(), session.to_string())].into())
                .build()
        };
        let empty = spec_with("");
        let named = spec_with(&format!("dup-{}", RunId::new()));
        let thread = protocol::SESSION_ID;
        let thread = named.metadata[thread].clone();
        let child = RunId::new();
        let started = protocol::EventPayload::ChildStarted {
            run_id: child,
            agent_id: AgentId::new(),
            limits: protocol::Limits {
                max_steps: 2,
                max_model_calls: 1,
            },
        };
        let event = |spec: &protocol::RunSpec, payload| {
            protocol::Event::record(
                protocol::EventSource::for_spec(
                    spec,
                    protocol::Actor::System,
                    protocol::Timestamp::now(),
                ),
                payload,
            )
        };
        let runs = vec![
            server::StoredRun {
                events: vec![event(&empty, protocol::EventPayload::RunCreated)],
                spec: empty.clone(),
            },
            server::StoredRun {
                events: vec![
                    event(&named, protocol::EventPayload::RunCreated),
                    event(&named, started.clone()),
                    event(&named, started),
                ],
                spec: named.clone(),
            },
        ];
        blocking(move || {
            for run in runs {
                store.put_run(run).expect("put");
            }
        })
        .await;
        let (_, threads) = server.get("/v1/threads", &user).await;
        let listed: Vec<&str> = threads["threads"]
            .as_array()
            .expect("threads")
            .iter()
            .map(|thread| thread["thread_id"].as_str().expect("id"))
            .collect();
        assert_eq!(listed, [thread.as_str()]);
        let (_, board) = server
            .get(&format!("/v1/threads/{thread}/board"), &user)
            .await;
        assert_eq!(board["cards"][0]["children"], json!([child]));
        assert!(board["cards"][0]["created_at"].is_number(), "{board}");
    }
}

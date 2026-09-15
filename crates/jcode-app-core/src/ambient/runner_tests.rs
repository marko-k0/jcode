use super::AmbientRunnerHandle;
use crate::ambient::{Priority, ScheduleTarget, ScheduledItem};
use crate::message::{Message, Role, StreamEvent, ToolDefinition};
use crate::provider::{EventStream, Provider};
use crate::session::Session;
use anyhow::Result;
use async_stream::stream;
use async_trait::async_trait;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

struct EnvVarGuard {
    key: &'static str,
    prev: Option<std::ffi::OsString>,
}

impl EnvVarGuard {
    fn set_path(key: &'static str, value: &std::path::Path) -> Self {
        let prev = std::env::var_os(key);
        crate::env::set_var(key, value);
        Self { key, prev }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        if let Some(prev) = self.prev.take() {
            crate::env::set_var(self.key, prev);
        } else {
            crate::env::remove_var(self.key);
        }
    }
}

struct TestProvider;

#[derive(Clone, Default)]
struct StreamingTestProvider {
    responses: Arc<StdMutex<VecDeque<Vec<StreamEvent>>>>,
}

impl StreamingTestProvider {
    fn queue_response(&self, events: Vec<StreamEvent>) {
        self.responses.lock().unwrap().push_back(events);
    }
}

#[async_trait]
impl Provider for TestProvider {
    async fn complete(
        &self,
        _messages: &[Message],
        _tools: &[ToolDefinition],
        _system: &str,
        _resume_session_id: Option<&str>,
    ) -> Result<EventStream> {
        Err(anyhow::anyhow!(
            "TestProvider should not be used for streaming completions in ambient runner tests"
        ))
    }

    fn name(&self) -> &str {
        "test"
    }

    fn fork(&self) -> Arc<dyn Provider> {
        Arc::new(TestProvider)
    }
}

#[async_trait]
impl Provider for StreamingTestProvider {
    async fn complete(
        &self,
        _messages: &[Message],
        _tools: &[ToolDefinition],
        _system: &str,
        _resume_session_id: Option<&str>,
    ) -> Result<EventStream> {
        let events = self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_default();
        let stream = stream! {
            for event in events {
                yield Ok(event);
            }
        };
        Ok(Box::pin(stream))
    }

    fn name(&self) -> &str {
        "test"
    }

    fn fork(&self) -> Arc<dyn Provider> {
        Arc::new(self.clone())
    }
}

#[tokio::test]
async fn runner_stays_alive_to_service_schedules_when_ambient_disabled() {
    let _guard = crate::storage::lock_test_env();
    let temp = tempfile::tempdir().expect("tempdir");
    let _home = EnvVarGuard::set_path("JCODE_HOME", temp.path());

    let provider: Arc<dyn Provider> = Arc::new(TestProvider);
    let runner = AmbientRunnerHandle::new(Arc::new(crate::safety::SafetySystem::new()));
    let task = tokio::spawn(runner.clone().run_loop(provider));

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        runner.is_running().await,
        "runner should remain active for scheduled tasks even with ambient disabled"
    );

    task.abort();
    let _ = task.await;
}

async fn assert_visible_launch_error_falls_back(error_kind: std::io::ErrorKind) {
    let _guard = crate::storage::lock_test_env();
    let temp = tempfile::tempdir().expect("tempdir");
    let _home = EnvVarGuard::set_path("JCODE_HOME", temp.path());

    let provider: Arc<dyn Provider> = Arc::new(StreamingTestProvider::default());
    let runner = AmbientRunnerHandle::new(Arc::new(crate::safety::SafetySystem::new()));
    let launch_attempted = Arc::new(AtomicBool::new(false));
    let launch_attempted_in_callback = launch_attempted.clone();

    let result = runner
        .run_cycle_with_visible_launcher(&provider, true, move || {
            launch_attempted_in_callback.store(true, Ordering::SeqCst);
            Err(std::io::Error::from(error_kind))
        })
        .await
        .expect("failed visible launch should continue as a headless cycle");

    assert!(launch_attempted.load(Ordering::SeqCst));
    assert!(
        result.conversation.is_some(),
        "headless fallback should capture an agent conversation"
    );
    assert!(
        result.summary.contains("forced end after 2 attempts"),
        "headless fallback should return the headless agent result"
    );
}

#[tokio::test]
async fn unsupported_visible_launch_falls_back_to_headless() {
    assert_visible_launch_error_falls_back(std::io::ErrorKind::Unsupported).await;
}

#[tokio::test]
async fn missing_visible_launcher_falls_back_to_headless() {
    assert_visible_launch_error_falls_back(std::io::ErrorKind::NotFound).await;
}

#[tokio::test]
async fn spawn_target_creates_one_child_session_and_runs_task() {
    let _guard = crate::storage::lock_test_env();
    let temp = tempfile::tempdir().expect("tempdir");
    let _home = EnvVarGuard::set_path("JCODE_HOME", temp.path());

    let provider = StreamingTestProvider::default();
    provider.queue_response(vec![
        StreamEvent::TextDelta("Spawned session handled task.".to_string()),
        StreamEvent::MessageEnd { stop_reason: None },
    ]);
    let provider: Arc<dyn Provider> = Arc::new(provider);

    let mut parent = Session::create_with_id(
        "session_parent_spawn_test".to_string(),
        None,
        Some("Parent".to_string()),
    );
    parent.working_dir = Some(temp.path().display().to_string());
    parent.save().expect("save parent session");

    let item = ScheduledItem {
        id: "sched_spawn_test".to_string(),
        scheduled_for: chrono::Utc::now(),
        context: "Follow up later".to_string(),
        priority: Priority::Normal,
        target: ScheduleTarget::Spawn {
            parent_session_id: parent.id.clone(),
        },
        created_by_session: parent.id.clone(),
        created_at: chrono::Utc::now(),
        working_dir: parent.working_dir.clone(),
        task_description: Some("Follow up later".to_string()),
        relevant_files: vec!["src/lib.rs".to_string()],
        git_branch: None,
        additional_context: Some("Background: spawned schedule test".to_string()),
    };

    let runner = AmbientRunnerHandle::new(Arc::new(crate::safety::SafetySystem::new()));
    let child_session_id = runner
        .spawn_session_for_scheduled_item(&provider, &item, &parent.id)
        .await
        .expect("spawned scheduled task should succeed");

    assert_ne!(child_session_id, parent.id);

    let child = Session::load(&child_session_id).expect("load spawned child session");
    assert_eq!(child.parent_id.as_deref(), Some(parent.id.as_str()));
    assert_eq!(child.working_dir, parent.working_dir);
    assert!(child.messages.iter().any(|message| {
        message.role == Role::User
            && message.content_preview().contains("[Scheduled task]")
            && message.content_preview().contains("Follow up later")
    }));
    assert!(child.messages.iter().any(|message| {
        message.role == Role::Assistant
            && message
                .content_preview()
                .contains("Spawned session handled task.")
    }));
}

/// A persisted `Running` status must not survive a reload.
///
/// A cycle that dies before its completion handler (crash, OOM, kill, hung
/// provider call) leaves `state.json` at `Running`. Since `should_run()`
/// returns false for `Running`, restoring it verbatim wedges ambient
/// permanently. Regression for the stale-Running recovery in
/// `AmbientState::load`.
#[test]
fn persisted_running_state_is_demoted_to_idle_on_load() {
    let _guard = crate::storage::lock_test_env();
    let temp = tempfile::tempdir().expect("tempdir");
    let _home = EnvVarGuard::set_path("JCODE_HOME", temp.path());

    let ambient_dir = temp.path().join("ambient");
    std::fs::create_dir_all(&ambient_dir).expect("ambient dir");
    let wedged = r#"{"status":{"Running":{"detail":"running agent"}}"#.to_string()
        + r#","last_run":null,"last_summary":null,"last_compactions":null,"#;
    std::fs::write(
        ambient_dir.join("state.json"),
        format!(
            "{}\"last_memories_modified\":null,\"total_cycles\":0}}",
            wedged
        ),
    )
    .expect("write wedged state");

    // Sanity: the fixture really is a Running state.
    let raw: crate::ambient::AmbientState =
        crate::storage::read_json(&ambient_dir.join("state.json")).expect("read fixture");
    assert!(
        matches!(raw.status, crate::ambient::AmbientStatus::Running { .. }),
        "fixture precondition: persisted state must be Running"
    );

    // The load path used by AmbientManager::new() must recover it.
    let loaded = crate::ambient::AmbientState::load().expect("load state");
    assert_eq!(
        loaded.status,
        crate::ambient::AmbientStatus::Idle,
        "a recovered Running status must be demoted to Idle, otherwise \
         should_run() can never return true again"
    );
    assert_eq!(loaded.total_cycles, 0);
}

/// Enabling ambient in config while a daemon is already running must be
/// picked up without a restart.
///
/// Regression for the boot-time `ambient_enabled` snapshot in `run_loop`. With
/// the snapshot, the status endpoint reported `enabled: true` (it reads live
/// config) while the loop stayed gated off forever.
///
/// Asserts on the loop's gate decision itself. An earlier version of this test
/// asserted that `state.status != Disabled` behind an `if reported_enabled`
/// guard; it passed on pristine upstream with the bug present, because it never
/// enabled ambient and never touched the gate. This one reads the same helper
/// `run_loop` calls and therefore fails when the read is hoisted out of the
/// loop.
#[test]
fn ambient_gate_observes_config_edited_after_loop_start() {
    let _guard = crate::storage::lock_test_env();
    let temp = tempfile::tempdir().expect("tempdir");
    let _home = EnvVarGuard::set_path("JCODE_HOME", temp.path());

    let path = crate::config::Config::path().expect("config path");
    std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");

    // Daemon boots with ambient disabled.
    std::fs::write(&path, "[ambient]\nenabled = false\n").expect("write");
    crate::config::Config::invalidate_cache();
    assert!(
        !crate::config::config().ambient.enabled,
        "precondition: ambient must start disabled"
    );

    // What `run_loop` used to capture once, before the loop (runner.rs:551).
    let boot_snapshot = crate::config::config().ambient.enabled;
    assert!(
        !crate::ambient::runner::ambient_allowed_now(crate::ambient::AmbientStatus::Idle),
        "precondition: gate must be closed while config says disabled"
    );

    // User enables ambient while the daemon keeps running. A different length
    // plus an extra line so the metadata fingerprint notices the edit even on
    // filesystems with coarse timestamp resolution.
    std::fs::write(&path, "[ambient]\nenabled = true\n# edited\n").expect("edit");
    crate::config::Config::invalidate_cache();

    // The status endpoint (runner.rs:252) reads live config and reports enabled.
    assert!(
        crate::config::config().ambient.enabled,
        "status endpoint would report enabled=true here"
    );

    // The loop's gate must agree. This is the actual regression assertion: it
    // fails if the gate still uses `boot_snapshot`.
    assert!(
        crate::ambient::runner::ambient_allowed_now(crate::ambient::AmbientStatus::Idle),
        "gate must observe the live config edit, not the boot-time snapshot \
         ({boot_snapshot}); otherwise ambient:status reports enabled=true while \
         the loop never runs a cycle"
    );
}

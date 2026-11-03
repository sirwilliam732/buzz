//! Local generation floors and owned work. A cancellation request is never a
//! drain proof: callers must retain tickets until output and children terminate.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::instrument::WithSubscriber;
use tracing::Instrument;

#[derive(Default)]
struct Host {
    floor: i64,
    issuer: Option<String>,
    cleanup_unproven: HashSet<uuid::Uuid>,
    work: HashMap<uuid::Uuid, (i64, Option<String>, CancellationToken)>,
}

/// Monotonic floors live for the process lifetime, independently of policy reads.
#[derive(Default)]
pub(crate) struct WorkTracker {
    hosts: Mutex<HashMap<String, Host>>,
    changed: Notify,
    reconcile: Notify,
    stopped: std::sync::atomic::AtomicBool,
    observed_requirement: std::sync::atomic::AtomicBool,
}

impl WorkTracker {
    /// A cluster hint invalidates only the worker snapshot, never authority.
    pub(crate) fn wake_policy(&self) {
        self.reconcile.notify_one();
    }

    /// Install before scanning. Admission and floor installation share the lock.
    pub(crate) fn require(&self, host: &str, generation: i64) {
        let mut hosts = self
            .hosts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = hosts.entry(host.to_owned()).or_default();
        entry.floor = entry.floor.max(generation);
        self.observed_requirement
            .store(true, std::sync::atomic::Ordering::Release);
        for (admitted, _, cancel) in entry.work.values() {
            if *admitted < entry.floor {
                cancel.cancel();
            }
        }
    }

    pub(crate) fn require_issuer(&self, host: &str, generation: i64, issuer: &str) {
        self.require(host, generation);
        let mut hosts = self
            .hosts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(h) = hosts.get_mut(host) {
            if generation == h.floor {
                h.issuer = Some(issuer.to_owned());
                for (g, admitted, cancel) in h.work.values() {
                    if *g < h.floor || admitted.as_deref() != Some(issuer) {
                        cancel.cancel();
                    }
                }
            }
        }
    }
    pub(crate) fn is_drained(&self, host: &str, generation: i64) -> bool {
        !self
            .hosts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(host)
            .is_some_and(|h| {
                h.work.values().any(|(g, issuer, _)| {
                    *g < generation
                        || h.issuer
                            .as_ref()
                            .is_some_and(|required| issuer.as_ref() != Some(required))
                })
            })
    }

    fn stop_and_empty(&self) -> bool {
        let hosts = self
            .hosts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.stopped
            .store(true, std::sync::atomic::Ordering::SeqCst);
        hosts.values().all(|h| h.work.is_empty())
    }
    pub(crate) fn floor(&self, host: &str) -> i64 {
        self.hosts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(host)
            .map_or(0, |entry| entry.floor)
    }

    /// Registration never upgrades a stale request into a compliant generation.
    pub(crate) fn enter(self: &Arc<Self>, host: &str, generation: i64) -> Option<WorkTicket> {
        let mut hosts = self
            .hosts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.stopped.load(std::sync::atomic::Ordering::SeqCst) {
            return None;
        }
        let entry = hosts.entry(host.to_owned()).or_default();
        if generation < entry.floor {
            return None;
        }
        let id = uuid::Uuid::new_v4();
        let cancel = CancellationToken::new();
        entry.work.insert(id, (generation, None, cancel.clone()));
        Some(WorkTicket(Arc::new(TicketInner {
            tracker: self.clone(),
            host: host.to_owned(),
            id,
            cancel,
        })))
    }

    /// New compliant work is excluded from this wait. No elapsed-time fallback.
    #[cfg(test)]
    pub(crate) async fn drained(&self, host: &str, generation: i64) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let pending = self
                .hosts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(host)
                .is_some_and(|h| {
                    h.work.values().any(|(g, issuer, _)| {
                        *g < generation
                            || h.issuer
                                .as_ref()
                                .is_some_and(|required| issuer.as_ref() != Some(required))
                    })
                });
            if !pending {
                return;
            }
            changed.await;
        }
    }
}

/// Clones retain the SAME accounting entry through child work and body output.
#[derive(Clone)]
pub(crate) struct WorkTicket(Arc<TicketInner>);
struct TicketInner {
    tracker: Arc<WorkTracker>,
    host: String,
    id: uuid::Uuid,
    cancel: CancellationToken,
}
impl WorkTicket {
    /// Only a cryptographically enforced admission can provide this provenance.
    pub(crate) fn enforce_issuer(&self, issuer: &str) -> bool {
        let mut hosts = self
            .0
            .tracker
            .hosts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(h) = hosts.get_mut(&self.0.host) else {
            return false;
        };
        let required = h.issuer.clone();
        let floor = h.floor;
        let Some((g, admitted, cancel)) = h.work.get_mut(&self.0.id) else {
            return false;
        };
        *admitted = Some(issuer.to_owned());
        if *g < floor || required.as_ref().is_some_and(|i| i != issuer) {
            cancel.cancel();
        }
        !cancel.is_cancelled()
    }
    /// Unknown producer termination permanently prevents this boot's drain ACK.
    pub(crate) fn cleanup_unproven(&self) {
        if let Some(host) = self
            .0
            .tracker
            .hosts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(&self.0.host)
        {
            host.cleanup_unproven.insert(self.0.id);
        }
    }
    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.0.cancel.clone()
    }
}
impl Drop for TicketInner {
    fn drop(&mut self) {
        if let Some(host) = self
            .tracker
            .hosts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(&self.host)
        {
            // A failed termination proof retains this exact generation's
            // obligation, rather than blocking later compliant generations.
            if !host.cleanup_unproven.contains(&self.id) {
                host.work.remove(&self.id);
            }
        }
        self.tracker.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn floor_closes_old_work_and_waits_for_last_child_without_blocking_reconnect() {
        let tracker = Arc::new(WorkTracker::default());
        let parent = tracker.enter("target", 0).unwrap();
        let child = parent.clone();
        let free = tracker.enter("control", 0).unwrap();
        tracker.require("target", 2);
        tracker.require("target", 1); // delayed read cannot lower the floor
        assert_eq!(tracker.floor("target"), 2);
        assert!(parent.cancellation().is_cancelled());
        assert!(!free.cancellation().is_cancelled());
        assert!(tracker.enter("target", 0).is_none());
        let current = tracker.enter("target", 2).unwrap();
        drop(parent);
        let wait = tracker.drained("target", 2);
        tokio::pin!(wait);
        assert!(futures_util::poll!(&mut wait).is_pending());
        drop(child);
        wait.await;
        assert!(!current.cancellation().is_cancelled());
    }
}

// Explicit task context: normal tokio::spawn does not inherit task locals.
tokio::task_local! { static WORK: WorkTicket; }
pub(crate) async fn with_ticket<F: std::future::Future>(
    ticket: WorkTicket,
    future: F,
) -> F::Output {
    WORK.scope(ticket, future).await
}
/// Capture work ownership for response producers whose Drop runs outside task context.
pub(crate) fn current_ticket() -> Option<WorkTicket> {
    WORK.try_with(Clone::clone).ok()
}
/// Child producers retain the parent's accounting even if its response drops.
pub(crate) fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    match WORK.try_with(Clone::clone) {
        Ok(ticket) => tokio::spawn(
            with_ticket(ticket, future)
                .in_current_span()
                .with_current_subscriber(),
        ),
        Err(_) => tokio::spawn(future.in_current_span().with_current_subscriber()),
    }
}
/// Forward cancellation without giving a detached watcher ownership of work.
pub(crate) fn link_cancel(
    control: crate::state::CommunityConnectionControl,
) -> Option<tokio_util::task::AbortOnDropHandle<()>> {
    let source = WORK.try_with(WorkTicket::cancellation).ok()?;
    let cancel = control.cancellation_token();
    Some(tokio_util::task::AbortOnDropHandle::new(tokio::spawn(
        async move {
            tokio::select! { _ = source.cancelled() => control.disconnect_auth_policy(), _ = cancel.cancelled() => {} }
        },
    )))
}

/// Post-upgrade policy races use a bounded, explicit 1008 close on both routes.
/// Handshake-time policy failures retain their normal HTTP denial response.
pub(crate) async fn deny_upgraded_socket(mut socket: axum::extract::ws::WebSocket) {
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        socket.send(crate::state::CommunityDisconnectReason::AuthPolicyUpgraded.close_message()),
    )
    .await;
}

pub(crate) fn spawn_on<F>(
    tracker: &tokio_util::task::TaskTracker,
    future: F,
) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    match WORK.try_with(Clone::clone) {
        Ok(ticket) => tracker.spawn(
            with_ticket(ticket, future)
                .in_current_span()
                .with_current_subscriber(),
        ),
        Err(_) => tracker.spawn(future.in_current_span().with_current_subscriber()),
    }
}

struct TicketBody {
    inner: axum::body::Body,
    ticket: WorkTicket,
}
impl http_body::Body for TicketBody {
    type Data = axum::body::Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        // Poll producers without holding the tracker mutex: producer drop or
        // task completion may release another work ticket. Serialize delivery
        // of the resulting frame with floor installation.
        let frame = std::pin::Pin::new(&mut self.inner).poll_frame(cx);
        let tracker = self.ticket.0.tracker.clone();
        let _hosts = tracker
            .hosts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.ticket.0.cancel.is_cancelled() {
            return std::task::Poll::Ready(None);
        }
        frame
    }
}
impl Drop for TicketBody {
    fn drop(&mut self) {
        use http_body::Body as _;
        if self.inner.is_end_stream() {
            return;
        }
        let mut inner = std::mem::replace(&mut self.inner, axum::body::Body::empty());
        let ticket = self.ticket.clone();
        // Finish/reap the producer without sending more bytes to the client.
        // Killing a subprocess on Drop alone is not proof it has terminated.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(with_ticket(ticket, async move {
                use http_body_util::BodyExt;
                while let Some(frame) = inner.frame().await {
                    if frame.is_err() {
                        break;
                    }
                }
            }));
        }
    }
}
/// Retain admission ownership and fence all protected HTTP response frames.
pub(crate) fn retain_body(
    response: axum::response::Response,
    ticket: WorkTicket,
) -> axum::response::Response {
    let (parts, inner) = response.into_parts();
    axum::http::Response::from_parts(parts, axum::body::Body::new(TicketBody { inner, ticket }))
}

/// Promote only the current enforced admission to its verified issuer.
pub(crate) fn enforce_current(issuer: &str) -> bool {
    WORK.try_with(|work| work.enforce_issuer(issuer))
        .unwrap_or(true)
}

impl crate::state::AppState {
    /// Observe primary policy before scanning dark sockets. This installs no
    /// boot/ACK evidence and cannot prove untracked producers have drained.
    pub(crate) fn observe_required_auth_policy(&self, community: uuid::Uuid, generation: i64) {
        self.auth_activation
            .require(&community.to_string(), generation);
        self.community_connections
            .require_untracked_auth(buzz_core::CommunityId::from_uuid(community));
    }

    pub(crate) fn auth_activation_tracking_enabled(&self) -> bool {
        self.config.nip_fi_activation_tracking_enabled
            || self.auth_activation_boot.get().is_some()
            || self
                .auth_activation
                .observed_requirement
                .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Called only after listeners stop and owned request/socket work has exited.
    /// A hard-shutdown or unfinished task leaves the boot pending for recovery.
    pub async fn retire_auth_activation(&self) -> buzz_db::Result<bool> {
        if !self
            .shutting_down
            .load(std::sync::atomic::Ordering::Acquire)
            || !self.auth_activation.stop_and_empty()
        {
            return Ok(false);
        }
        match self.auth_activation_boot.get() {
            Some(boot) => self.db.retire_auth_boot(*boot).await,
            None => Ok(false),
        }
    }

    /// Stop ticket admission, then wait for every owned producer before retiring.
    /// The caller must retain its hard-exit backstop; unknown termination stays
    /// pending instead of being converted to retirement by a timeout.
    pub async fn drain_and_retire_auth_activation(&self) -> buzz_db::Result<()> {
        if self.auth_activation_boot.get().is_none() {
            return Ok(());
        }
        metrics::counter!("buzz_nip_fi_activation_retirement_attempts_total").increment(1);
        loop {
            let changed = self.auth_activation.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.retire_auth_activation().await? {
                metrics::counter!("buzz_nip_fi_activation_retirement_success_total").increment(1);
                return Ok(());
            }
            changed.await;
        }
    }

    /// Establish dark bypass or register before listener admission, then reconcile
    /// missed policy notifications. `None` means a proven unarmed scope with both
    /// rollout switches off; any writer/registration failure must prevent serving.
    /// Reconciliation runs every five seconds. No lease expiry or fleet eviction.
    /// The production verifier is absent, so baseline completion stays disabled.
    pub async fn start_auth_activation(
        self: &Arc<Self>,
    ) -> buzz_db::Result<Option<tokio_util::task::AbortOnDropHandle<()>>> {
        let boot = uuid::Uuid::new_v4();
        if self.auth_activation_boot.get().is_some() {
            return Err(buzz_db::DbError::AccessDenied(
                "activation already registered".into(),
            ));
        }
        let image = std::env::var("BUZZ_RELAY_IMAGE_DIGEST")
            .unwrap_or_else(|_| format!("unverified-version:{}", env!("CARGO_PKG_VERSION")));
        let Some(targets) = self
            .db
            .prepare_auth_boot(
                boot,
                &image,
                self.config.nip_fi_activation_tracking_enabled
                    || self.config.nip_fi_upgrades_enabled,
            )
            .await?
        else {
            metrics::gauge!("buzz_nip_fi_activation_tracking_enabled").set(0.0);
            return Ok(None);
        };
        metrics::gauge!("buzz_nip_fi_activation_tracking_enabled").set(1.0);
        metrics::counter!("buzz_nip_fi_activation_registration_success_total").increment(1);
        let _ = self.auth_activation_boot.set(boot);
        for target in &targets {
            self.auth_activation.require_issuer(
                &target.target_community_id.to_string(),
                target.policy_generation,
                &target.expected_issuer,
            );
        }
        let state = self.clone();
        Ok(Some(tokio_util::task::AbortOnDropHandle::new(
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = tick.tick() => {},
                        _ = state.auth_activation.reconcile.notified() => {},
                    }
                    match state.db.auth_activation_targets(boot).await {
                        Ok(targets) => {
                            metrics::gauge!("buzz_nip_fi_activation_local_obligations_pending")
                                .set(targets.len() as f64);
                            let oldest = targets
                                .iter()
                                .map(|t| (chrono::Utc::now() - t.accepted_at).num_seconds().max(0))
                                .max()
                                .unwrap_or(0);
                            metrics::gauge!("buzz_nip_fi_activation_oldest_pending_seconds")
                                .set(oldest as f64);
                            // Install every floor before inspecting any drain result.
                            for target in &targets {
                                state.auth_activation.require_issuer(
                                    &target.target_community_id.to_string(),
                                    target.policy_generation,
                                    &target.expected_issuer,
                                );
                            }
                            for target in targets {
                                if state.auth_activation.is_drained(
                                    &target.target_community_id.to_string(),
                                    target.policy_generation,
                                ) {
                                    let ack = buzz_db::community_auth::DrainAck::new(
                                        boot,
                                        target.operation_id,
                                        target.policy_generation,
                                    );
                                    if let Err(error) = state.db.acknowledge_auth_drain(ack).await {
                                        tracing::warn!(%error,"activation ACK remains pending");
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            tracing::warn!(%error,"activation reconciliation remains pending")
                        }
                    }
                }
            }),
        )))
    }
}

#[cfg(test)]
mod cost_tests;
#[cfg(test)]
mod postgres_tests;

#[cfg(test)]
mod ownership_tests {
    #[test]
    fn observed_dark_requirement_catches_late_untracked_registration_only() {
        let registry = crate::state::CommunityConnectionRegistry::new();
        let target = buzz_core::CommunityId::from_uuid(uuid::Uuid::new_v4());
        let unrelated = buzz_core::CommunityId::from_uuid(uuid::Uuid::new_v4());
        let old = crate::state::CommunityConnectionControl::new(CancellationToken::new());
        let free = crate::state::CommunityConnectionControl::new(CancellationToken::new());
        let _old = registry.register(uuid::Uuid::new_v4(), target, old.clone());
        let _free = registry.register(uuid::Uuid::new_v4(), unrelated, free.clone());
        assert_eq!(registry.require_untracked_auth(target), 1);
        assert!(old.cancellation_token().is_cancelled());
        assert!(!free.cancellation_token().is_cancelled());
        let late = crate::state::CommunityConnectionControl::new_audio(CancellationToken::new());
        let _late = registry.register(uuid::Uuid::new_v4(), target, late.clone());
        assert!(late.cancellation_token().is_cancelled());
        let tracker = Arc::new(WorkTracker::default());
        let ticket = tracker.enter(&target.as_uuid().to_string(), 1).unwrap();
        let current = WORK.sync_scope(ticket, || {
            crate::state::CommunityConnectionControl::new(CancellationToken::new())
        });
        let _current = registry.register(uuid::Uuid::new_v4(), target, current.clone());
        assert_eq!(registry.require_untracked_auth(target), 0);
        assert!(!current.cancellation_token().is_cancelled());
    }

    #[test]
    fn process_audio_drain_is_sticky_without_cancelling_root_jitter() {
        let registry = crate::state::CommunityConnectionRegistry::new();
        let community = buzz_core::CommunityId::from_uuid(uuid::Uuid::new_v4());
        let root = crate::state::CommunityConnectionControl::new(
            tokio_util::sync::CancellationToken::new(),
        );
        let audio = crate::state::CommunityConnectionControl::new_audio(
            tokio_util::sync::CancellationToken::new(),
        );
        let _root = registry.register(uuid::Uuid::new_v4(), community, root.clone());
        let _audio = registry.register(uuid::Uuid::new_v4(), community, audio.clone());
        assert_eq!(registry.drain_audio(), 1);
        assert!(audio.cancellation_token().is_cancelled());
        assert_eq!(
            *audio.disconnect_reason().borrow(),
            Some(crate::state::CommunityDisconnectReason::RelayRestart)
        );
        assert!(!root.cancellation_token().is_cancelled());
        let late = crate::state::CommunityConnectionControl::new_audio(
            tokio_util::sync::CancellationToken::new(),
        );
        let _late = registry.register(uuid::Uuid::new_v4(), community, late.clone());
        assert!(late.cancellation_token().is_cancelled());
    }

    use super::*;
    use http_body_util::BodyExt;

    #[tokio::test]
    async fn shadow_and_other_issuer_are_legacy_but_matching_reconnect_survives() {
        let tracker = Arc::new(WorkTracker::default());
        let shadow = tracker.enter("target", 0).unwrap();
        let enterprise = tracker.enter("target", 0).unwrap();
        assert!(enterprise.enforce_issuer("issuer"));
        tracker.require_issuer("target", 1, "issuer");
        assert!(shadow.cancellation().is_cancelled());
        assert!(enterprise.cancellation().is_cancelled()); // conservative reconnect
        let wrong = tracker.enter("target", 1).unwrap();
        assert!(!wrong.enforce_issuer("other"));
        let current = tracker.enter("target", 1).unwrap();
        assert!(current.enforce_issuer("issuer"));
        drop((shadow, enterprise, wrong));
        assert!(tracker.is_drained("target", 1));
        assert!(!current.cancellation().is_cancelled());
    }

    #[tokio::test]
    async fn failed_cleanup_keeps_only_its_own_generation_pending() {
        let tracker = Arc::new(WorkTracker::default());
        let old = tracker.enter("target", 0).unwrap();
        tracker.require("target", 1);
        let current = tracker.enter("target", 1).unwrap();
        current.cleanup_unproven();
        drop(current);
        drop(old);
        assert!(
            tracker.is_drained("target", 1),
            "compliant work cannot block the old generation"
        );
        tracker.require("target", 2);
        assert!(
            !tracker.is_drained("target", 2),
            "unknown termination cannot disappear on drop"
        );
        assert!(!tracker.stop_and_empty());
    }

    #[tokio::test]
    async fn cancellation_fences_body_output_and_task_join_retains_ownership() {
        let tracker = Arc::new(WorkTracker::default());
        let ticket = tracker.enter("target", 0).unwrap();
        let tasks = tokio_util::task::TaskTracker::new();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let child = WORK.sync_scope(ticket.clone(), || {
            spawn_on(&tasks, async {
                release_rx.await.unwrap();
            })
        });
        let response = axum::http::Response::new(axum::body::Body::from("protected"));
        let response = retain_body(response, ticket);
        tracker.require("target", 1);
        tasks.close();
        let joined = tasks.wait();
        tokio::pin!(joined);
        assert!(futures_util::poll!(&mut joined).is_pending());
        assert!(!tracker.is_drained("target", 1));
        assert!(response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .is_empty());
        assert!(!tracker.is_drained("target", 1));
        release_tx.send(()).unwrap();
        child.await.unwrap();
        joined.await;
        tracker.drained("target", 1).await;
        assert!(tracker.stop_and_empty());
        assert!(tracker.enter("target", 1).is_none());
    }
}

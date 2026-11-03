use super::*;

#[tokio::test]
#[ignore = "requires Postgres"]
async fn relay_ticket_drain_persists_exact_ack_with_only_baseline_pending() {
    use buzz_db::community_auth::{DrainAck, UpgradeOutcome, UpgradeRequest};
    let db = buzz_db::Db::from_pool(
        sqlx::PgPool::connect(&crate::test_support::database_url())
            .await
            .unwrap(),
    );
    let keys = nostr::Keys::generate();
    let owner = keys.public_key().to_hex();
    let issuer = "https://activation.example";
    let host = format!("activation-{}.example", uuid::Uuid::new_v4());
    let created = db
        .create_community_with_owner_and_nip_fi_issuers(&host, &owner, &[issuer.to_owned()], &keys)
        .await
        .unwrap();
    let buzz_db::CreateCommunityWithOwnerResult::Created(row) = created.outcome else {
        panic!("create");
    };
    let boot = uuid::Uuid::new_v4();
    db.register_auth_boot(boot, "relay-fixture").await.unwrap();
    let tracker = Arc::new(WorkTracker::default());
    let parent = tracker.enter(&host, 0).unwrap();
    // A producer outlives the parent response: dropping that response cannot ACK.
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let producer = WORK.sync_scope(parent.clone(), || {
        spawn(async {
            ready_tx.send(()).unwrap();
            release_rx.await.unwrap();
        })
    });
    ready_rx.await.unwrap();
    let request = UpgradeRequest {
        operation_id: uuid::Uuid::new_v4(),
        community_id: *row.id.as_uuid(),
        expected_host: host.clone(),
        actor_pubkey: owner.clone(),
        expected_owner_pubkey: owner.clone(),
        expected_issuer: issuer.into(),
        operator_pubkey: owner,
    };
    let UpgradeOutcome::Accepted(record) =
        db.enable_community_nip_fi(&request, Ok(())).await.unwrap()
    else {
        panic!("enable");
    };
    tracker.require_issuer(&host, record.policy_generation, issuer);
    assert!(parent.cancellation().is_cancelled());
    drop(parent);
    assert!(!tracker.is_drained(&host, record.policy_generation));
    release_tx.send(()).unwrap();
    producer.await.unwrap();
    tracker.drained(&host, record.policy_generation).await;
    let ack = DrainAck::new(boot, record.operation_id, record.policy_generation);
    for wrong in [
        DrainAck::new(
            uuid::Uuid::new_v4(),
            record.operation_id,
            record.policy_generation,
        ),
        DrainAck::new(boot, uuid::Uuid::new_v4(), record.policy_generation),
        DrainAck::new(boot, record.operation_id, record.policy_generation + 1),
    ] {
        assert!(!db.acknowledge_auth_drain(wrong).await.unwrap());
    }
    assert!(db.acknowledge_auth_drain(ack).await.unwrap());
    let status = db
        .community_auth_upgrade_status(record.operation_id, &request.operator_pubkey)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.state, "pending");
    assert_eq!(status.pending_reasons, ["registry_baseline_unverified"]);
    let satisfied: bool = sqlx::query_scalar("SELECT satisfied_at IS NOT NULL FROM community_auth_obligations WHERE boot_id=$1 AND operation_id=$2 AND policy_generation=$3")
        .bind(boot).bind(record.operation_id).bind(record.policy_generation).fetch_one(db.pool()).await.unwrap();
    assert!(satisfied);
    // The dependency build has no test authority: writing plausible baseline
    // data cannot bypass the missing production verifier.
    sqlx::query("UPDATE community_auth_baseline SET verified=true,epoch=epoch+1,evidence='{\"unchecked\":true}' WHERE singleton")
        .execute(db.pool()).await.unwrap();
    assert!(db.acknowledge_auth_drain(ack).await.unwrap());
    let status = db
        .community_auth_upgrade_status(record.operation_id, &request.operator_pubkey)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.state, "pending");
    assert_eq!(status.pending_reasons, ["registry_baseline_unverified"]);
    assert!(status.activated_at.is_none());
}

struct HeldWorkflowOutput {
    entered: tokio::sync::mpsc::UnboundedSender<()>,
    release: Arc<Notify>,
}
impl buzz_workflow::ActionSink for HeldWorkflowOutput {
    fn send_message(
        &self,
        _: buzz_core::CommunityId,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: Option<&str>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<String, buzz_workflow::ActionSinkError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.entered.send(()).unwrap();
            self.release.notified().await;
            Ok("00".repeat(32))
        })
    }
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn event_workflow_spawn_retains_generation_until_output_and_finalization_exit() {
    let db = buzz_db::Db::from_pool(
        sqlx::PgPool::connect(&crate::test_support::database_url())
            .await
            .unwrap(),
    );
    let keys = nostr::Keys::generate();
    let owner = keys.public_key();
    let host = format!("workflow-drain-{}.example", uuid::Uuid::new_v4());
    let created = db
        .create_community_with_owner(&host, &owner.to_hex(), &keys)
        .await
        .unwrap();
    let buzz_db::CreateCommunityWithOwnerResult::Created(row) = created.outcome else {
        panic!("create");
    };
    db.ensure_user(row.id, &owner.to_bytes()).await.unwrap();
    let channel = uuid::Uuid::new_v4();
    db.create_channel_with_id(
        row.id,
        channel,
        "workflow-drain",
        buzz_db::channel::ChannelType::Stream,
        buzz_db::channel::ChannelVisibility::Open,
        None,
        &owner.to_bytes(),
        None,
    )
    .await
    .unwrap();
    let definition = serde_json::json!({"name":"drain", "trigger":{"on":"message_posted"},
        "steps":[{"id":"s1","action":"send_message","text":"held"}],"enabled":true})
    .to_string();
    let workflow = db
        .create_workflow(
            row.id,
            Some(channel),
            &owner.to_bytes(),
            "drain",
            &definition,
            &[0u8; 32],
        )
        .await
        .unwrap();
    let engine = Arc::new(buzz_workflow::WorkflowEngine::new(
        db.clone(),
        buzz_workflow::WorkflowConfig::default(),
    ));
    let (entered, mut received) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    engine.set_action_sink(Arc::new(HeldWorkflowOutput {
        entered,
        release: release.clone(),
    }));
    let event = nostr::EventBuilder::new(nostr::Kind::Custom(9), "trigger")
        .sign_with_keys(&keys)
        .unwrap();
    let event = buzz_core::StoredEvent::new(event, Some(channel));
    let tracker = Arc::new(WorkTracker::default());
    let key = row.id.as_uuid().to_string();
    let ticket = tracker.enter(&key, 0).unwrap();
    with_ticket(
        ticket.clone(),
        engine.on_event_with_spawner(row.id, &event, |task| {
            spawn(task);
        }),
    )
    .await
    .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), received.recv())
        .await
        .unwrap()
        .unwrap();
    tracker.require(&key, 1);
    drop(ticket);
    assert!(
        !tracker.is_drained(&key, 1),
        "the engine's detached output still owns the old generation"
    );
    release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(10), tracker.drained(&key, 1))
        .await
        .unwrap();
    let finished: bool = sqlx::query_scalar("SELECT completed_at IS NOT NULL FROM workflow_runs WHERE community_id=$1 AND workflow_id=$2")
        .bind(row.id.as_uuid()).bind(workflow).fetch_one(db.pool()).await.unwrap();
    assert!(
        finished,
        "ACK eligibility follows durable workflow finalization"
    );
}

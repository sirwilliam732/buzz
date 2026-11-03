use super::*;
use crate::CreateCommunityWithOwnerResult;
use nostr::Keys;

const ISSUER: &str = "https://identity.example/api/goose";

async fn fixture() -> (Db, UpgradeRequest, Keys) {
    let db = Db::from_pool(
        sqlx::PgPool::connect(&crate::test_support::database_url())
            .await
            .unwrap(),
    );
    let keys = Keys::generate();
    let owner = Keys::generate().public_key().to_hex();
    let host = format!("auth-{}.example", Uuid::new_v4().simple());
    let write = db
        .create_community_with_owner_and_nip_fi_issuers(&host, &owner, &[ISSUER.to_owned()], &keys)
        .await
        .unwrap();
    let CreateCommunityWithOwnerResult::Created(record) = write.outcome else {
        panic!("create failed")
    };
    let request = UpgradeRequest {
        operation_id: Uuid::new_v4(),
        community_id: *record.id.as_uuid(),
        expected_host: host,
        actor_pubkey: owner.clone(),
        expected_owner_pubkey: owner,
        expected_issuer: ISSUER.to_owned(),
        operator_pubkey: keys.public_key().to_hex(),
    };
    (db, request, keys)
}

async fn accepted(db: &Db, request: &UpgradeRequest) -> UpgradeRecord {
    let UpgradeOutcome::Accepted(record) =
        db.enable_community_nip_fi(request, Ok(())).await.unwrap()
    else {
        panic!("acceptance refused")
    };
    *record
}

async fn policy(db: &Db, request: &UpgradeRequest) -> CommunityAuthPolicy {
    db.community_auth_policy(CommunityId::from_uuid(request.community_id))
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn response_loss_recovery_and_payload_binding() {
    let (db, request, _) = fixture().await;
    assert!(!policy(&db, &request).await.requires_nip_fi);
    let first = accepted(&db, &request).await;
    assert_eq!(first.state, "pending");
    assert_eq!(first.policy_generation, 1);
    // Discard the POST result and recover by original operator/UUID.
    assert_eq!(
        db.community_auth_upgrade_status(request.operation_id, &request.operator_pubkey)
            .await
            .unwrap(),
        Some(first.clone())
    );
    assert_eq!(accepted(&db, &request).await, first);
    assert!(db
        .community_auth_upgrade_status(request.operation_id, "another operator")
        .await
        .unwrap()
        .is_none());
    for field in 0..6 {
        let mut changed = request.clone();
        match field {
            0 => changed.community_id = Uuid::new_v4(),
            1 => changed.expected_host = "another.example".to_owned(),
            2 => changed.actor_pubkey = "b".repeat(64),
            3 => changed.expected_owner_pubkey = "c".repeat(64),
            4 => changed.expected_issuer = "https://another.example".to_owned(),
            _ => changed.operator_pubkey = "d".repeat(64),
        }
        assert_eq!(
            db.enable_community_nip_fi(&changed, Ok(())).await.unwrap(),
            UpgradeOutcome::Refused("idempotency_conflict")
        );
    }
    assert_eq!(policy(&db, &request).await.policy_generation, 1);
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn concurrent_ids_and_duplicate_requests_converge() {
    let (db, request, _) = fixture().await;
    let mut second = request.clone();
    second.operation_id = Uuid::new_v4();
    let (a, b, c) = tokio::join!(
        accepted(&db, &request),
        accepted(&db, &request),
        accepted(&db, &second)
    );
    assert_eq!(a, b);
    assert_eq!(a.policy_generation, c.policy_generation);
    assert_eq!(policy(&db, &request).await.policy_generation, 1);
    let (_, mut other, _) = fixture().await;
    other.operation_id = request.operation_id;
    assert_eq!(
        db.enable_community_nip_fi(&other, Ok(())).await.unwrap(),
        UpgradeOutcome::Refused("idempotency_conflict")
    );
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn failed_preconditions_leave_free_policy_and_no_receipt() {
    let (db, request, _) = fixture().await;
    for (mut changed, code) in [
        (request.clone(), "host_conflict"),
        (request.clone(), "owner_conflict"),
        (request.clone(), "issuer_conflict"),
        (request.clone(), "community_not_found"),
    ] {
        match code {
            "host_conflict" => changed.expected_host = "wrong.example".to_owned(),
            "owner_conflict" => changed.actor_pubkey = "b".repeat(64),
            "issuer_conflict" => changed.expected_issuer = "https://wrong.example".to_owned(),
            _ => changed.community_id = Uuid::new_v4(),
        }
        assert_eq!(
            db.enable_community_nip_fi(&changed, Ok(())).await.unwrap(),
            UpgradeOutcome::Refused(code)
        );
    }
    assert_eq!(
        db.enable_community_nip_fi(&request, Err("authorization_unavailable"))
            .await
            .unwrap(),
        UpgradeOutcome::Refused("authorization_unavailable")
    );
    db.unbind_nip_fi_issuer(CommunityId::from_uuid(request.community_id), ISSUER, true)
        .await
        .unwrap();
    assert_eq!(
        db.enable_community_nip_fi(&request, Ok(())).await.unwrap(),
        UpgradeOutcome::Refused("issuer_conflict")
    );
    assert_eq!(
        policy(&db, &request).await,
        CommunityAuthPolicy {
            community_id: request.community_id,
            requires_nip_fi: false,
            policy_generation: 0
        }
    );
    assert!(db
        .community_auth_upgrade_status(request.operation_id, &request.operator_pubkey)
        .await
        .unwrap()
        .is_none());
}

// Observe the actual PostgreSQL blocker and statement, rather than treating
// elapsed time or either possible race outcome as evidence of serialization.
async fn blocked_upgrade(
    db: &Db,
    request: &UpgradeRequest,
    locker_pid: i32,
    statement: &str,
) -> tokio::task::JoinHandle<Result<UpgradeOutcome>> {
    use std::str::FromStr;
    let application = format!("upgrade-lock-{}", request.operation_id.simple());
    let options = sqlx::postgres::PgConnectOptions::from_str(&crate::test_support::database_url())
        .unwrap()
        .application_name(&application);
    let upgrade_db = Db::from_pool(
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap(),
    );
    let request = request.clone();
    let task =
        tokio::spawn(async move { upgrade_db.enable_community_nip_fi(&request, Ok(())).await });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            assert!(
                !task.is_finished(),
                "enable escaped before taking its precondition lock"
            );
            let row: Option<(String, Vec<i32>)> = sqlx::query_as(
                "SELECT query, pg_blocking_pids(pid) FROM pg_stat_activity
                 WHERE application_name = $1 AND wait_event_type = 'Lock'",
            )
            .bind(&application)
            .fetch_optional(&db.pool)
            .await
            .unwrap();
            if let Some((query, blockers)) = row {
                if blockers.contains(&locker_pid) {
                    let query = query.split_whitespace().collect::<Vec<_>>().join(" ");
                    assert!(
                        query.starts_with(statement),
                        "enable reached a later statement without the precondition lock: {query}"
                    );
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("observe the enable statement blocked by the held transaction");
    task
}

async fn assert_refused_without_mutation(
    db: &Db,
    request: &UpgradeRequest,
    task: tokio::task::JoinHandle<Result<UpgradeOutcome>>,
    code: &'static str,
) {
    assert_eq!(task.await.unwrap().unwrap(), UpgradeOutcome::Refused(code));
    assert!(db
        .community_auth_upgrade_status(request.operation_id, &request.operator_pubkey)
        .await
        .unwrap()
        .is_none());
    let policy: (bool, i64) =
        sqlx::query_as("SELECT requires_nip_fi, policy_generation FROM communities WHERE id = $1")
            .bind(request.community_id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(policy, (false, 0));
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn owner_lock_blocks_uncommitted_owner_change_then_refuses_enable() {
    let (db, request, _) = fixture().await;
    let mut locker = db.pool.begin().await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *locker)
        .await
        .unwrap();
    sqlx::query("UPDATE relay_members SET role = 'member' WHERE community_id = $1 AND pubkey = $2")
        .bind(request.community_id)
        .bind(&request.expected_owner_pubkey)
        .execute(&mut *locker)
        .await
        .unwrap();
    let task = blocked_upgrade(&db, &request, pid, "SELECT pubkey FROM relay_members").await;
    locker.commit().await.unwrap();
    assert_refused_without_mutation(&db, &request, task, "owner_conflict").await;
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn community_lock_blocks_uncommitted_archive_then_refuses_enable() {
    let (db, request, _) = fixture().await;
    let mut locker = db.pool.begin().await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *locker)
        .await
        .unwrap();
    sqlx::query("UPDATE communities SET archived_at = now() WHERE id = $1")
        .bind(request.community_id)
        .execute(&mut *locker)
        .await
        .unwrap();
    let task = blocked_upgrade(&db, &request, pid, "SELECT host, archived_at").await;
    locker.commit().await.unwrap();
    assert_refused_without_mutation(&db, &request, task, "lifecycle_conflict").await;
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn issuer_lock_blocks_uncommitted_unbind_then_refuses_enable() {
    let (db, request, _) = fixture().await;
    let mut locker = db.pool.begin().await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *locker)
        .await
        .unwrap();
    sqlx::query("DELETE FROM community_nip_fi_issuers WHERE community_id = $1 AND issuer = $2")
        .bind(request.community_id)
        .bind(ISSUER)
        .execute(&mut *locker)
        .await
        .unwrap();
    let task = blocked_upgrade(
        &db,
        &request,
        pid,
        "SELECT issuer FROM community_nip_fi_issuers",
    )
    .await;
    locker.commit().await.unwrap();
    assert_refused_without_mutation(&db, &request, task, "issuer_conflict").await;
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn accepted_receipt_survives_transfer_without_granting_fresh_owner_authority() {
    let (db, request, keys) = fixture().await;
    let first = accepted(&db, &request).await;
    let new_owner = Keys::generate().public_key().to_hex();
    let _ = db
        .transfer_ownership(
            CommunityId::from_uuid(request.community_id),
            &new_owner,
            &request.expected_owner_pubkey,
            &keys,
        )
        .await
        .unwrap();
    assert_eq!(accepted(&db, &request).await, first);
    let mut fresh = request.clone();
    fresh.operation_id = Uuid::new_v4();
    assert_eq!(
        db.enable_community_nip_fi(&fresh, Ok(())).await.unwrap(),
        UpgradeOutcome::Refused("owner_conflict")
    );
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn authoritative_lookup_failure_is_error_not_fabricated_acceptance() {
    let db = Db::from_pool(
        sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_millis(100))
            .connect_lazy("postgres://127.0.0.1:1/unavailable")
            .unwrap(),
    );
    let (_, request, _) = fixture().await;
    assert!(db.enable_community_nip_fi(&request, Ok(())).await.is_err());
    assert!(db
        .community_auth_upgrade_status(request.operation_id, &request.operator_pubkey)
        .await
        .is_err());
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn deletion_intent_refuses_enable_before_quiescing() {
    let (db, request, _) = fixture().await;
    db.deletion_store()
        .submit(
            Some(&request.expected_host),
            "operator",
            None,
            Some(CommunityId::from_uuid(request.community_id)),
        )
        .await
        .unwrap();
    assert_eq!(
        db.enable_community_nip_fi(&request, Ok(())).await.unwrap(),
        UpgradeOutcome::Refused("lifecycle_conflict")
    );
    assert!(!policy(&db, &request).await.requires_nip_fi);
    assert!(db
        .community_auth_upgrade_status(request.operation_id, &request.operator_pubkey)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn migration_schema_expansion_preserves_existing_optional_rows_with_and_without_trust() {
    let pool = sqlx::PgPool::connect(&crate::test_support::database_url())
        .await
        .unwrap();
    crate::migration::run_migrations_through(&pool, 81)
        .await
        .unwrap();
    let db = Db::from_pool(pool.clone());
    let free = db
        .ensure_configured_community("free-before.example")
        .await
        .unwrap()
        .id;
    let bound = db
        .ensure_configured_community_with_nip_fi_issuers(
            "bound-before.example",
            &[ISSUER.to_owned()],
        )
        .await
        .unwrap()
        .id;
    crate::migration::run_migrations(&pool).await.unwrap();
    for community in [free, bound] {
        assert_eq!(
            db.community_auth_policy(community).await.unwrap(),
            Some(CommunityAuthPolicy {
                community_id: *community.as_uuid(),
                requires_nip_fi: false,
                policy_generation: 0,
            })
        );
    }
    assert_eq!(
        db.lookup_nip_fi_binding_by_host("bound-before.example")
            .await
            .unwrap()
            .unwrap()
            .issuers,
        vec![ISSUER.to_owned()]
    );
    let receipts: i64 = sqlx::query_scalar("SELECT count(*) FROM community_auth_upgrades")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(receipts, 0);
    let global: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM _operator_global_tables WHERE table_name = 'community_auth_upgrades')")
        .fetch_one(&pool).await.unwrap();
    assert!(global);
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn supported_writer_maintains_generation_while_direct_sql_is_not_constrained() {
    let (db, mut request, _) = fixture().await;
    assert_eq!(policy(&db, &request).await.policy_generation, 0);
    sqlx::query("UPDATE communities SET policy_generation = -1 WHERE id = $1")
        .bind(request.community_id)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(policy(&db, &request).await.policy_generation, -1);
    sqlx::query("UPDATE communities SET policy_generation = DEFAULT WHERE id = $1")
        .bind(request.community_id)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(policy(&db, &request).await.policy_generation, 0);
    let first = accepted(&db, &request).await;
    assert_eq!(first.policy_generation, 1);
    request.operation_id = Uuid::new_v4();
    assert_eq!(accepted(&db, &request).await.policy_generation, 1);
    let invalid_receipt = sqlx::query(
        "UPDATE community_auth_upgrades SET policy_generation = 0 WHERE operation_id = $1",
    )
    .bind(request.operation_id)
    .execute(db.pool())
    .await
    .unwrap_err();
    assert_eq!(
        invalid_receipt
            .as_database_error()
            .unwrap()
            .code()
            .as_deref(),
        Some("23514")
    );
    assert_eq!(policy(&db, &request).await.policy_generation, 1);
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn host_policy_tracks_primary_requirement_and_serving_lifecycle() {
    let (db, request, _) = fixture().await;
    let host = &request.expected_host;
    assert_eq!(
        db.community_auth_policy_for_host(host).await.unwrap(),
        Some(CommunityAuthPolicy {
            community_id: request.community_id,
            requires_nip_fi: false,
            policy_generation: 0
        })
    );
    let upgraded = accepted(&db, &request).await;
    assert_eq!(
        db.community_auth_policy_for_host(host).await.unwrap(),
        Some(CommunityAuthPolicy {
            community_id: request.community_id,
            requires_nip_fi: true,
            policy_generation: upgraded.policy_generation
        })
    );
    assert!(db
        .community_auth_policy_for_host("unknown.example")
        .await
        .unwrap()
        .is_none());
    sqlx::query("INSERT INTO community_deletion_requests (community_id, community_host, requested_by) VALUES ($1, $2, 'policy-test')")
        .bind(request.community_id).bind(host).execute(db.pool()).await.unwrap();
    for stage in ["submitted", "inventoried", "approved"] {
        sqlx::query("UPDATE community_deletion_requests SET stage = $2 WHERE community_id = $1")
            .bind(request.community_id)
            .bind(stage)
            .execute(db.pool())
            .await
            .unwrap();
        assert!(db.lookup_community_by_host(host).await.unwrap().is_some());
        assert!(
            db.community_auth_policy_for_host(host)
                .await
                .unwrap()
                .unwrap()
                .requires_nip_fi,
            "{stage}"
        );
    }
    sqlx::query("UPDATE communities SET archived_at = now() WHERE id = $1")
        .bind(request.community_id)
        .execute(db.pool())
        .await
        .unwrap();
    assert!(db
        .community_auth_policy_for_host(host)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn activation_exact_ack_baseline_invalidation_and_restart_journey() {
    let (db, request, _) = fixture().await;
    let boot = Uuid::new_v4();
    assert!(db
        .register_auth_boot(boot, "test-image")
        .await
        .unwrap()
        .is_empty());
    let record = accepted(&db, &request).await;
    let generation = record.policy_generation;
    let targets = db.auth_activation_targets(boot).await.unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].operation_id, request.operation_id);
    let ack = DrainAck::new(boot, request.operation_id, generation);
    for wrong in [
        DrainAck::new(Uuid::new_v4(), request.operation_id, generation),
        DrainAck::new(boot, Uuid::new_v4(), generation),
        DrainAck::new(boot, request.operation_id, generation + 1),
    ] {
        assert!(!db.acknowledge_auth_drain(wrong).await.unwrap());
    }
    // A replacement can apply its own state, but cannot discharge a paused or
    // unreachable original boot, even if the replacement then self-retires.
    let replacement = Uuid::new_v4();
    db.register_auth_boot(replacement, "replacement-while-original-paused")
        .await
        .unwrap();
    assert!(db
        .acknowledge_auth_drain(DrainAck::new(replacement, request.operation_id, generation))
        .await
        .unwrap());
    assert!(db.retire_auth_boot(replacement).await.unwrap());
    let held = db
        .community_auth_upgrade_status(request.operation_id, &request.operator_pubkey)
        .await
        .unwrap()
        .unwrap();
    assert!(held
        .pending_reasons
        .iter()
        .any(|reason| reason == "boot_drain_pending"));
    // Resuming the original boot must still ACK its exact incarnation.
    assert!(db.acknowledge_auth_drain(ack).await.unwrap());
    let status = db
        .community_auth_upgrade_status(request.operation_id, &request.operator_pubkey)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.state, "pending");
    assert_eq!(status.pending_reasons, ["registry_baseline_unverified"]);
    let satisfied: bool = sqlx::query_scalar("SELECT satisfied_at IS NOT NULL FROM community_auth_obligations WHERE operation_id=$1 AND boot_id=$2 AND policy_generation=$3")
        .bind(request.operation_id).bind(boot).bind(generation).fetch_one(db.pool()).await.unwrap();
    assert!(satisfied);
    // Baseline unverified is not a reason to repeat an already satisfied ACK.
    assert!(db.auth_activation_targets(boot).await.unwrap().is_empty());
    db.invalidate_auth_baseline().await.unwrap();
    assert!(db.auth_activation_targets(boot).await.unwrap().is_empty());
    super::activation::verify_baseline_for_test(&db).await;
    assert!(db.acknowledge_auth_drain(ack).await.unwrap());
    let active = db
        .community_auth_upgrade_status(request.operation_id, &request.operator_pubkey)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active.state, "active");
    assert!(active.activated_at.is_some());
    assert!(active.pending_reasons.is_empty());
    assert_eq!(active.activation_history.as_array().unwrap().len(), 1);
    assert_eq!(active.activation_history[0]["baseline"]["test_only"], true);
    // A new incarnation cannot reuse the old boot identity. A new registration
    // joins existing operations before it can serve, temporarily pending again.
    assert!(db.register_auth_boot(boot, "replacement").await.is_err());
    let restart = Uuid::new_v4();
    assert_eq!(
        db.register_auth_boot(restart, "replacement")
            .await
            .unwrap()
            .len(),
        1
    );
    let pending = db
        .community_auth_upgrade_status(request.operation_id, &request.operator_pubkey)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.state, "pending");
    assert_eq!(pending.pending_reasons, ["boot_drain_pending"]);
    assert!(db.retire_auth_boot(restart).await.unwrap());
    assert!(!db
        .acknowledge_auth_drain(DrainAck::new(restart, request.operation_id, generation))
        .await
        .unwrap());
    assert!(db
        .register_auth_boot(restart, "cannot-resume")
        .await
        .is_err());
    assert_eq!(
        db.community_auth_upgrade_status(request.operation_id, &request.operator_pubkey)
            .await
            .unwrap()
            .unwrap()
            .state,
        "active"
    );
    db.invalidate_auth_baseline().await.unwrap();
    let invalidated = db
        .community_auth_upgrade_status(request.operation_id, &request.operator_pubkey)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(invalidated.state, "pending");
    assert_eq!(invalidated.activated_at, active.activated_at);
    assert_eq!(invalidated.activation_epoch, active.activation_epoch);
    assert_eq!(invalidated.activation_history, active.activation_history);
    // Revalidation records a new epoch without erasing the earlier proof.
    super::activation::verify_baseline_for_test(&db).await;
    assert!(db.acknowledge_auth_drain(ack).await.unwrap());
    let reactivated = db
        .community_auth_upgrade_status(request.operation_id, &request.operator_pubkey)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reactivated.state, "active");
    assert_eq!(reactivated.activated_at, active.activated_at);
    assert_eq!(reactivated.activation_history.as_array().unwrap().len(), 2);
    // Lifecycle ineligibility outranks a missing baseline.
    sqlx::query("UPDATE communities SET archived_at=now() WHERE id=$1")
        .bind(request.community_id)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        db.community_auth_upgrade_status(request.operation_id, &request.operator_pubkey)
            .await
            .unwrap()
            .unwrap()
            .state,
        "unavailable"
    );
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn activation_registration_and_lifecycle_lock_order_journey() {
    // Both registration orders must capture the same serving participants.
    let (db, request, _) = fixture().await;
    let boot = Uuid::new_v4();
    let (registration, enabled) = tokio::join!(
        db.register_auth_boot(boot, "racing-boot"),
        db.enable_community_nip_fi(&request, Ok(()))
    );
    registration.unwrap();
    let UpgradeOutcome::Accepted(record) = enabled.unwrap() else {
        panic!("enable");
    };
    let obligations: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM community_auth_obligations WHERE operation_id=$1 AND boot_id=$2",
    )
    .bind(record.operation_id)
    .bind(boot)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(obligations, 1);
    super::activation::verify_baseline_for_test(&db).await;
    let ack = DrainAck::new(boot, record.operation_id, record.policy_generation);
    let (acked, invalidated) = tokio::join!(
        db.acknowledge_auth_drain(ack),
        db.invalidate_auth_baseline()
    );
    assert!(acked.unwrap());
    invalidated.unwrap();
    assert_eq!(
        db.community_auth_upgrade_status(record.operation_id, &request.operator_pubkey)
            .await
            .unwrap()
            .unwrap()
            .pending_reasons,
        ["registry_baseline_unverified"]
    );

    // Exercise the actual opposing lifecycle methods against both enable and
    // completion. A cycle fails via the SQL lock bound or the outer deadline.
    for lifecycle in ["archive", "delete", "transfer"] {
        for completing in [false, true] {
            let (db, request, keys) = fixture().await;
            let boot = Uuid::new_v4();
            db.register_auth_boot(boot, "lifecycle-race").await.unwrap();
            let record = if completing {
                Some(accepted(&db, &request).await)
            } else {
                None
            };
            let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
            let auth = async {
                barrier.wait().await;
                if let Some(record) = record {
                    db.acknowledge_auth_drain(DrainAck::new(
                        boot,
                        record.operation_id,
                        record.policy_generation,
                    ))
                    .await
                    .unwrap();
                } else {
                    db.enable_community_nip_fi(&request, Ok(())).await.unwrap();
                }
            };
            let mutation = async {
                barrier.wait().await;
                match lifecycle {
                    "archive" => {
                        db.archive_community_owned_by(
                            &request.expected_host,
                            &request.expected_owner_pubkey,
                            "deployment.example",
                        )
                        .await
                        .unwrap();
                    }
                    "delete" => {
                        db.deletion_store()
                            .submit(
                                Some(&request.expected_host),
                                "operator",
                                None,
                                Some(CommunityId::from_uuid(request.community_id)),
                            )
                            .await
                            .unwrap();
                    }
                    _ => {
                        let _ = db
                            .transfer_ownership(
                                CommunityId::from_uuid(request.community_id),
                                &Keys::generate().public_key().to_hex(),
                                &request.expected_owner_pubkey,
                                &keys,
                            )
                            .await
                            .unwrap();
                    }
                }
            };
            tokio::time::timeout(std::time::Duration::from_secs(15), async {
                tokio::join!(auth, mutation);
            })
            .await
            .unwrap();
            if lifecycle != "transfer" {
                if let Some(status) = db
                    .community_auth_upgrade_status(request.operation_id, &request.operator_pubkey)
                    .await
                    .unwrap()
                {
                    assert_eq!(status.state, "unavailable");
                    assert!(status.pending_reasons.is_empty());
                }
            }
        }
    }
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn dark_bypass_is_atomic_and_armed_or_operation_scopes_cannot_disable_tracking() {
    let (db, request, _) = fixture().await;
    let skipped = Uuid::new_v4();
    assert!(db
        .prepare_auth_boot(skipped, "dark", false)
        .await
        .unwrap()
        .is_none());
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM community_auth_boots WHERE boot_id=$1")
            .bind(skipped)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(count, 0);
    // Dark opt-in crashes remain registered; no absence/age-based discharge.
    let crashed = Uuid::new_v4();
    db.prepare_auth_boot(crashed, "pre-arming-crash", true)
        .await
        .unwrap()
        .unwrap();
    accepted(&db, &request).await;
    let boot = Uuid::new_v4();
    assert!(db
        .prepare_auth_boot(boot, "off-after-operation", false)
        .await
        .unwrap()
        .is_some());
    assert_eq!(db.auth_activation_targets(crashed).await.unwrap().len(), 1);
    db.invalidate_auth_baseline().await.unwrap();
    assert!(db
        .prepare_auth_boot(Uuid::new_v4(), "off-after-invalidation", false)
        .await
        .unwrap()
        .is_some());
}

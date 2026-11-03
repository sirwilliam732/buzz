mod postgres_tests {
    use super::super::*;

    // Real TCP + writer DB: client cancellation cannot hide an old handler
    // or body producer from durable reconciliation and its exact boot ACK.
    #[tokio::test]
    #[ignore = "requires Postgres"]
    async fn activation_waits_for_disconnected_http_handler_and_body_producer() {
        use buzz_db::community_auth::{UpgradeOutcome, UpgradeRequest};
        let base = real_db_state().await.unwrap();
        let mut configured = (*base).clone();
        Arc::make_mut(&mut configured.config).nip_fi.mode = buzz_auth::NipFiMode::Off;
        let state = Arc::new(configured);
        let keys = nostr::Keys::generate();
        let owner = keys.public_key().to_hex();
        let host = format!("http-drain-{}.example", uuid::Uuid::new_v4());
        let issuer = "https://activation.example";
        let created = state
            .db
            .create_community_with_owner_and_nip_fi_issuers(
                &host,
                &owner,
                &[issuer.to_owned()],
                &keys,
            )
            .await
            .unwrap();
        let buzz_db::CreateCommunityWithOwnerResult::Created(row) = created.outcome else {
            panic!("create");
        };
        let reconcile = state.start_auth_activation().await.unwrap();
        let handler_release = Arc::new(tokio::sync::Notify::new());
        let body_release = Arc::new(tokio::sync::Notify::new());
        let (entered, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
        let (producing, mut producing_rx) = tokio::sync::mpsc::unbounded_channel();
        let app = Router::new()
            .route(
                "/held",
                get({
                    let handler_release = handler_release.clone();
                    let body_release = body_release.clone();
                    move || {
                        let handler_release = handler_release.clone();
                        let body_release = body_release.clone();
                        let entered = entered.clone();
                        let producing = producing.clone();
                        async move {
                            entered.send(()).unwrap();
                            handler_release.notified().await;
                            Body::from_stream(futures_util::stream::once(async move {
                                producing.send(()).unwrap();
                                body_release.notified().await;
                                Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"old output"))
                            }))
                        }
                    }
                }),
            )
            .layer(middleware::from_fn_with_state(
                state.clone(),
                nip_fi_assertion_guard,
            ));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let request_host = host.clone();
        let request = tokio::spawn(async move {
            reqwest::Client::new()
                .get(format!("http://{address}/held"))
                .header("host", request_host)
                .send()
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(10), entered_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let upgrade = UpgradeRequest {
            operation_id: uuid::Uuid::new_v4(),
            community_id: *row.id.as_uuid(),
            expected_host: host.clone(),
            actor_pubkey: owner.clone(),
            expected_owner_pubkey: owner.clone(),
            expected_issuer: issuer.into(),
            operator_pubkey: owner,
        };
        let UpgradeOutcome::Accepted(record) = state
            .db
            .enable_community_nip_fi(&upgrade, Ok(()))
            .await
            .unwrap()
        else {
            panic!("enable");
        };
        // Same local consumer used by the cluster cache-invalidation bus.
        // The hint carries no authority; the worker must read the durable row.
        state.apply_cache_invalidation(
            row.id,
            buzz_pubsub::cache_invalidation::CacheInvalidation::CommunityAuthPolicy,
        );
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while state.auth_activation.floor(&row.id.as_uuid().to_string())
                < record.policy_generation
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        request.abort();
        let _ = request.await;
        assert!(!state
            .auth_activation
            .is_drained(&row.id.as_uuid().to_string(), record.policy_generation));
        handler_release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(10), producing_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(!state
            .auth_activation
            .is_drained(&row.id.as_uuid().to_string(), record.policy_generation));
        body_release.notify_one();
        let boot = *state.auth_activation_boot.get().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let satisfied: bool = sqlx::query_scalar("SELECT satisfied_at IS NOT NULL FROM community_auth_obligations WHERE boot_id=$1 AND operation_id=$2 AND policy_generation=$3")
                .bind(boot).bind(record.operation_id).bind(record.policy_generation).fetch_one(state.db.pool()).await.unwrap();
            if satisfied { break; }
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
        let status = state
            .db
            .community_auth_upgrade_status(record.operation_id, &upgrade.operator_pubkey)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.pending_reasons, ["registry_baseline_unverified"]);
        // Reusing a host for a different immutable community cannot inherit the
        // old target's sticky floor or satisfy its generation obligations.
        sqlx::query("UPDATE communities SET host=$2 WHERE id=$1")
            .bind(row.id.as_uuid())
            .bind(format!("old-{host}"))
            .execute(state.db.pool())
            .await
            .unwrap();
        let replacement = state
            .db
            .create_community_with_owner_and_nip_fi_issuers(
                &host,
                &keys.public_key().to_hex(),
                &[],
                &keys,
            )
            .await
            .unwrap();
        let buzz_db::CreateCommunityWithOwnerResult::Created(replacement) = replacement.outcome
        else {
            panic!("replacement");
        };
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, host.parse().unwrap());
        let policy = crate::nip_fi_policy::resolve(&state, &headers)
            .await
            .unwrap();
        assert_eq!(policy.community_id, *replacement.id.as_uuid());
        assert_eq!(policy.generation, 0);
        assert!(!policy.required);
        assert!(state
            .auth_activation
            .enter(&policy.community_id.to_string(), 0)
            .is_some());
        assert_eq!(
            state.auth_activation.floor(&row.id.as_uuid().to_string()),
            record.policy_generation
        );
        drop(reconcile);
        server.abort();
    }
}

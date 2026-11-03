mod postgres_tests {
    use super::super::*;
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};
    type Client = tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >;

    async fn frame(client: &mut Client, stage: &str) -> Message {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let message = client.next().await.unwrap().unwrap();
                match message {
                    Message::Ping(_) => {
                        client.flush().await.unwrap();
                    }
                    Message::Pong(_) => {}
                    message => return message,
                }
            }
        })
        .await
        .unwrap_or_else(|error| panic!("{stage}: {error}"))
    }

    async fn journey(audio: bool) {
        use buzz_db::community_auth::{UpgradeOutcome, UpgradeRequest};
        let base = real_db_state().await.unwrap();
        let mut configured = (*base).clone();
        let config = Arc::make_mut(&mut configured.config);
        config.nip_fi.mode = buzz_auth::NipFiMode::Off;
        config.nip_fi_activation_tracking_enabled = true;
        let redis_url = std::env::var("REDIS_URL").unwrap();
        let redis_pool = deadpool_redis::Config::from_url(&redis_url)
            .create_pool(Some(deadpool_redis::Runtime::Tokio1))
            .unwrap();
        configured.admission_rate_limiter =
            Arc::new(buzz_pubsub::rate_limiter::RedisRateLimiter::new(redis_pool));
        let state = Arc::new(configured);
        let keys = nostr::Keys::generate();
        let owner = keys.public_key().to_hex();
        let host = format!("socket-drain-{}.example", uuid::Uuid::new_v4());
        let issuer = "https://activation.example";
        let created = state
            .db
            .create_community_with_owner_and_nip_fi_issuers(&host, &owner, &[issuer.into()], &keys)
            .await
            .unwrap();
        let buzz_db::CreateCommunityWithOwnerResult::Created(row) = created.outcome else {
            panic!("create");
        };
        state
            .db
            .ensure_user(row.id, &keys.public_key().to_bytes())
            .await
            .unwrap();
        let channel = uuid::Uuid::new_v4();
        state
            .db
            .create_channel_with_id(
                row.id,
                channel,
                "activation",
                buzz_db::channel::ChannelType::Stream,
                buzz_db::channel::ChannelVisibility::Open,
                None,
                &keys.public_key().to_bytes(),
                None,
            )
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_members(community_id,channel_id,pubkey,role) VALUES($1,$2,$3,'member') ON CONFLICT DO NOTHING")
            .bind(row.id.as_uuid()).bind(channel).bind(keys.public_key().to_bytes().to_vec()).execute(state.db.pool()).await.unwrap();
        let _worker = state.start_auth_activation().await.unwrap().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = build_router(state.clone())
            .into_make_service_with_connect_info::<std::net::SocketAddr>();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let path = if audio {
            format!("/huddle/{channel}/audio")
        } else {
            "/".into()
        };
        let mut req = format!("ws://{addr}{path}").into_client_request().unwrap();
        req.headers_mut().insert("host", host.parse().unwrap());
        let (mut client, _) = tokio_tungstenite::connect_async(req).await.unwrap();
        let challenge = frame(&mut client, "challenge").await;
        let Message::Text(challenge) = challenge else {
            panic!("challenge {challenge:?}")
        };
        let challenge: serde_json::Value = serde_json::from_str(&challenge).unwrap();
        let challenge = if audio {
            challenge["challenge"].as_str().unwrap()
        } else {
            challenge[1].as_str().unwrap()
        };
        let auth = nostr::EventBuilder::new(nostr::Kind::Authentication, "")
            .tag(nostr::Tag::parse(["relay", &format!("ws://{host}")]).unwrap())
            .tag(nostr::Tag::parse(["challenge", challenge]).unwrap())
            .sign_with_keys(&keys)
            .unwrap();
        let auth = if audio {
            serde_json::json!({"type":"auth","event":auth,"parent_channel_id":null,"protocol_version":1})
        } else {
            serde_json::json!(["AUTH", auth])
        };
        client
            .send(Message::Text(auth.to_string().into()))
            .await
            .unwrap();
        loop {
            let msg = frame(&mut client, "AUTH result").await;
            let Message::Text(text) = msg else {
                panic!("admission {msg:?}")
            };
            let value: serde_json::Value = serde_json::from_str(&text).unwrap();
            if audio && value["type"] == "joined" {
                break;
            }
            if !audio && value[0] == "OK" {
                assert_eq!(value[2], true, "{value}");
                break;
            }
        }
        if !audio {
            client
                .send(Message::Text(
                    r#"["REQ","live",{"kinds":[1],"limit":1}]"#.into(),
                ))
                .await
                .unwrap();
            loop {
                if let Message::Text(t) = frame(&mut client, "EOSE for initial subscription").await
                {
                    assert!(!t.contains("CLOSED"), "initial subscription refused: {t}");
                    if t.contains("EOSE") {
                        break;
                    }
                }
            }
            assert_eq!(state.sub_registry.total_subscriptions(), 1);
        }
        let (held, release) = if audio {
            crate::nip_fi_test_hooks::audio_teardown_cleanup_hook::arm(row.id)
        } else {
            // Holds a second real spawned REQ handler before its effect permit.
            crate::nip_fi_test_hooks::req_registration_hook::arm(row.id)
        };
        if !audio {
            client
                .send(Message::Text(
                    r#"["REQ","held",{"kinds":[1],"limit":1}]"#.into(),
                ))
                .await
                .unwrap();
        }
        let mut held = Some(held);
        if !audio {
            tokio::time::timeout(std::time::Duration::from_secs(10), held.take().unwrap())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(state.sub_registry.total_subscriptions(), 1);
        }
        let request = UpgradeRequest {
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
            .enable_community_nip_fi(&request, Ok(()))
            .await
            .unwrap()
        else {
            panic!("enable")
        };
        state.invalidate_community_auth_policy(&buzz_core::tenant::TenantContext::resolved(
            row.id,
            host.clone(),
        ));
        loop {
            if let Message::Close(Some(close)) = frame(&mut client, "policy close").await {
                assert_eq!(u16::from(close.code), 1008);
                assert_eq!(close.reason, "community authentication upgraded");
                let _ = client.flush().await;
                break;
            }
        }
        if audio {
            tokio::time::timeout(std::time::Duration::from_secs(10), held.take().unwrap())
                .await
                .unwrap()
                .unwrap();
        }
        let boot = *state.auth_activation_boot.get().unwrap();
        let satisfied = || async {
            sqlx::query_scalar::<_,bool>("SELECT satisfied_at IS NOT NULL FROM community_auth_obligations WHERE boot_id=$1 AND operation_id=$2 AND policy_generation=$3")
                .bind(boot).bind(record.operation_id).bind(record.policy_generation).fetch_one(state.db.pool()).await.unwrap()
        };
        assert!(!satisfied().await, "close/cancel is not a drain ACK");
        let mut reconnect = format!("ws://{addr}{path}").into_client_request().unwrap();
        reconnect
            .headers_mut()
            .insert("host", host.parse().unwrap());
        let error = tokio_tungstenite::connect_async(reconnect)
            .await
            .unwrap_err();
        let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
            panic!("expected HTTP denial {error:?}")
        };
        assert!(matches!(response.status().as_u16(), 401 | 503));
        state.begin_shutdown();
        assert!(
            !state.retire_auth_activation().await.unwrap(),
            "held work prevents retirement too"
        );
        release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !satisfied().await {
                tokio::task::yield_now().await
            }
        })
        .await
        .unwrap();
        assert_eq!(state.sub_registry.total_subscriptions(), 0);
        assert_eq!(
            state
                .pubsub
                .topic_refcount(
                    &buzz_core::tenant::TenantContext::resolved(row.id, host.clone()),
                    buzz_pubsub::EventTopic::Global
                )
                .await,
            0
        );
        if audio {
            assert!(state
                .audio_rooms
                .get(row.id, channel)
                .is_none_or(|room| room.roster_snapshot().peers.is_empty()));
        }
        state.drain_and_retire_auth_activation().await.unwrap();
        let retired: bool = sqlx::query_scalar(
            "SELECT retired_at IS NOT NULL FROM community_auth_boots WHERE boot_id=$1",
        )
        .bind(boot)
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert!(retired);
        server.abort();
    }
    #[tokio::test]
    #[ignore = "requires Postgres"]
    async fn root_policy_close_waits_for_req_join_subscription_cleanup_and_exact_ack() {
        journey(false).await;
    }
    #[tokio::test]
    #[ignore = "requires Postgres"]
    async fn audio_policy_close_waits_for_peer_cleanup_and_exact_ack() {
        journey(true).await;
    }
    async fn authenticated_socket(
        addr: std::net::SocketAddr,
        host: &str,
        path: &str,
        key: &nostr::Keys,
        assertion: Option<&str>,
        audio: bool,
    ) -> Client {
        let mut request = format!("ws://{addr}{path}").into_client_request().unwrap();
        request.headers_mut().insert("host", host.parse().unwrap());
        if let Some(token) = assertion {
            request.headers_mut().insert(
                "nostr-federated-identity",
                format!("Bearer {token}").parse().unwrap(),
            );
        }
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let Message::Text(challenge) = frame(&mut client, "challenge").await else {
            panic!("challenge")
        };
        let challenge: serde_json::Value = serde_json::from_str(&challenge).unwrap();
        let challenge = if audio {
            challenge["challenge"].as_str().unwrap()
        } else {
            challenge[1].as_str().unwrap()
        };
        let event = nostr::EventBuilder::new(nostr::Kind::Authentication, "")
            .tag(nostr::Tag::parse(["relay", &format!("ws://{host}")]).unwrap())
            .tag(nostr::Tag::parse(["challenge", challenge]).unwrap())
            .sign_with_keys(key)
            .unwrap();
        let auth = if audio {
            serde_json::json!({"type":"auth","event":event,"parent_channel_id":null,"protocol_version":1})
        } else {
            serde_json::json!(["AUTH", event])
        };
        client
            .send(Message::Text(auth.to_string().into()))
            .await
            .unwrap();
        loop {
            let Message::Text(text) = frame(&mut client, "authenticated socket").await else {
                panic!("admission closed")
            };
            let value: serde_json::Value = serde_json::from_str(&text).unwrap();
            if audio && value["type"] == "joined" {
                break;
            }
            if !audio && value[0] == "OK" {
                assert_eq!(value[2], true, "{value}");
                break;
            }
            assert!(value["type"] != "error", "{value}");
        }
        client
    }

    async fn dark_observation_journey(bind_first: bool) {
        use crate::nip_fi_core::test_support::{
            assertion_verifier, sign_assertion, TEST_COMMUNITY_URI, TEST_ISSUER,
        };
        use buzz_db::community_auth::{UpgradeOutcome, UpgradeRequest};
        let base = real_db_state().await.unwrap();
        let mut configured = (*base).clone();
        let config = Arc::make_mut(&mut configured.config);
        config.nip_fi.mode = buzz_auth::NipFiMode::Off;
        config.nip_fi_activation_tracking_enabled = false;
        config.nip_fi_upgrades_enabled = false;
        config.nip_fi.max_connection_lifetime_secs = 3600;
        config.nip_fi.communities = crate::nip_fi_config::NipFiCommunities::for_test_any_host(
            TEST_COMMUNITY_URI,
            &[TEST_ISSUER],
        );
        configured.nip_fi_verifier = Some(Arc::new(assertion_verifier(
            buzz_auth::TokenClass::DedicatedNipFi,
        )));
        configured.nip_fi_deny_map = Some(Arc::new(buzz_auth::NipFiDenyMap::new(
            16,
            vec![buzz_auth::IssuerCapacity {
                issuer: TEST_ISSUER.into(),
                capacity: 16,
            }],
        )));
        let redis = deadpool_redis::Config::from_url(std::env::var("REDIS_URL").unwrap())
            .create_pool(Some(deadpool_redis::Runtime::Tokio1))
            .unwrap();
        configured.admission_rate_limiter =
            Arc::new(buzz_pubsub::rate_limiter::RedisRateLimiter::new(redis));
        let state = Arc::new(configured);
        assert!(state.start_auth_activation().await.unwrap().is_none());
        let key = nostr::Keys::generate();
        let owner = key.public_key().to_hex();
        let host = format!("dark-target-{}.example", uuid::Uuid::new_v4());
        let created = state
            .db
            .create_community_with_owner_and_nip_fi_issuers(
                &host,
                &owner,
                &[TEST_ISSUER.into()],
                &key,
            )
            .await
            .unwrap();
        let buzz_db::CreateCommunityWithOwnerResult::Created(row) = created.outcome else {
            panic!("target")
        };
        let free_host = format!("dark-free-{}.example", uuid::Uuid::new_v4());
        let _ = state
            .db
            .create_community_with_owner(&free_host, &owner, &key)
            .await
            .unwrap();
        state
            .db
            .ensure_user(row.id, &key.public_key().to_bytes())
            .await
            .unwrap();
        let channel = uuid::Uuid::new_v4();
        state
            .db
            .create_channel_with_id(
                row.id,
                channel,
                "dark-audio",
                buzz_db::channel::ChannelType::Stream,
                buzz_db::channel::ChannelVisibility::Open,
                None,
                &key.public_key().to_bytes(),
                None,
            )
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_members(community_id,channel_id,pubkey,role) VALUES($1,$2,$3,'member') ON CONFLICT DO NOTHING")
            .bind(row.id.as_uuid()).bind(channel).bind(key.public_key().to_bytes().to_vec()).execute(state.db.pool()).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = build_router(state.clone())
            .into_make_service_with_connect_info::<std::net::SocketAddr>();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let path = format!("/huddle/{channel}/audio");
        let mut root = authenticated_socket(addr, &host, "/", &key, None, false).await;
        let mut audio = authenticated_socket(addr, &host, &path, &key, None, true).await;
        let mut free = authenticated_socket(addr, &free_host, "/", &key, None, false).await;
        assert!(!state.auth_activation_tracking_enabled());
        let request = UpgradeRequest {
            operation_id: uuid::Uuid::new_v4(),
            community_id: *row.id.as_uuid(),
            expected_host: host.clone(),
            actor_pubkey: owner.clone(),
            expected_owner_pubkey: owner.clone(),
            expected_issuer: TEST_ISSUER.into(),
            operator_pubkey: owner,
        };
        let UpgradeOutcome::Accepted(record) = state
            .db
            .enable_community_nip_fi(&request, Ok(()))
            .await
            .unwrap()
        else {
            panic!("enable")
        };
        // No reconcile worker exists. A real FI reconnect makes the first durable
        // observation and must close the older untracked sockets without itself closing.
        if bind_first {
            crate::nip_fi_policy::bind_for_upgrade(&state, &host)
                .await
                .unwrap();
        }
        let token = sign_assertion("nip-fi+jwt", &key.public_key().to_hex(), None);
        let compliant_before_close = if bind_first {
            None
        } else {
            Some(authenticated_socket(addr, &host, "/", &key, Some(&token), false).await)
        };
        for client in [&mut root, &mut audio] {
            let Message::Close(Some(close)) = frame(client, "dark legacy policy close").await
            else {
                panic!("expected defined close")
            };
            assert_eq!(u16::from(close.code), 1008);
            assert_eq!(close.reason, "community authentication upgraded");
            let _ = client.flush().await;
        }
        // A later reconnect must not mask a missing binding observation.
        let mut compliant = match compliant_before_close {
            Some(client) => client,
            None => authenticated_socket(addr, &host, "/", &key, Some(&token), false).await,
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while state
                .audio_rooms
                .get(row.id, channel)
                .is_some_and(|r| !r.roster_snapshot().peers.is_empty())
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // Last-peer cleanup normally ends the old huddle; use a fresh huddle
        // to test policy admission/continued service independently of room expiry.
        let next_channel = uuid::Uuid::new_v4();
        state
            .db
            .create_channel_with_id(
                row.id,
                next_channel,
                "compliant-audio",
                buzz_db::channel::ChannelType::Stream,
                buzz_db::channel::ChannelVisibility::Open,
                None,
                &key.public_key().to_bytes(),
                None,
            )
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_members(community_id,channel_id,pubkey,role) VALUES($1,$2,$3,'member') ON CONFLICT DO NOTHING")
            .bind(row.id.as_uuid()).bind(next_channel).bind(key.public_key().to_bytes().to_vec()).execute(state.db.pool()).await.unwrap();
        let mut compliant_audio = authenticated_socket(
            addr,
            &host,
            &format!("/huddle/{next_channel}/audio"),
            &key,
            Some(&token),
            true,
        )
        .await;
        // Repeat observation, then exercise live sockets. This must neither
        // cancel compliant reconnects nor touch the unrelated free community.
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("host", host.parse().unwrap());
        crate::nip_fi_policy::resolve(&state, &headers)
            .await
            .unwrap();
        for client in [&mut compliant, &mut compliant_audio, &mut free] {
            ping_round_trip(client).await;
        }
        assert!(state.auth_activation_boot.get().is_none());
        let obligations: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM community_auth_obligations WHERE operation_id=$1",
        )
        .bind(record.operation_id)
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(obligations, 0);
        let status = state
            .db
            .community_auth_upgrade_status(record.operation_id, &request.operator_pubkey)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.state, "pending");
        assert!(status.activated_at.is_none());
        drop((compliant, compliant_audio, free));
        server.abort();
    }

    #[tokio::test]
    #[ignore = "requires Postgres"]
    async fn dark_observation_closes_existing_root_and_audio_without_ack_and_preserves_reconnects()
    {
        dark_observation_journey(false).await;
    }

    #[tokio::test]
    #[ignore = "requires Postgres"]
    async fn dark_binding_observation_closes_existing_root_and_audio_without_ack() {
        dark_observation_journey(true).await;
    }

    async fn ping_round_trip(client: &mut Client) {
        client
            .send(Message::Ping(bytes::Bytes::from_static(b"live")))
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                match client.next().await.unwrap().unwrap() {
                    Message::Pong(bytes) => {
                        assert_eq!(bytes.as_ref(), b"live");
                        break;
                    }
                    Message::Ping(_) => {
                        client.flush().await.unwrap();
                    }
                    Message::Close(close) => panic!("unexpected close {close:?}"),
                    _ => {}
                }
            }
        })
        .await
        .unwrap();
    }
}

//! Force an old discovery producer to finish after a newer metadata edit.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use nostr::{EventBuilder, Keys, Kind, Tag};
use tokio::sync::oneshot;
use uuid::Uuid;

use super::group_state_postgres_tests::{
    community, create_channel, edit_channel, group_state_tags, http_auth, state,
};
use super::ingest::ingest_event;
use super::side_effects::emit_group_discovery_events;

type Pause = (oneshot::Sender<()>, oneshot::Receiver<()>);

fn pauses() -> &'static Mutex<HashMap<Uuid, Pause>> {
    static PAUSES: OnceLock<Mutex<HashMap<Uuid, Pause>>> = OnceLock::new();
    PAUSES.get_or_init(Default::default)
}

/// Consume a channel-scoped, one-shot pause; other producers can proceed.
pub(super) async fn after_capture(channel: Uuid) {
    let pause = pauses().lock().unwrap().remove(&channel);
    if let Some((captured, resume)) = pause {
        let _ = captured.send(());
        let _ = resume.await;
    }
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn delayed_discovery_cannot_restore_stale_metadata() {
    let state = state().await;
    let tenant = community(&state, "discovery-ordering").await;
    let owner = Keys::generate();
    let channel = create_channel(&state, &tenant, &owner, &[]).await;
    state
        .db
        .add_relay_member(
            tenant.community(),
            &owner.public_key().to_hex(),
            "owner",
            None,
            &Keys::generate(),
        )
        .await
        .map(|write| write.outcome)
        .expect("seed relay owner");
    edit_channel(&state, &tenant, &owner, channel, &[&["auto_join", "true"]])
        .await
        .expect("enable auto_join");
    let has_auto_join = |tags: &[Vec<String>]| tags.iter().any(|tag| tag[0] == "auto_join");
    assert!(has_auto_join(
        &group_state_tags(&state, &tenant, channel, 39000).await
    ));

    let (captured, did_capture) = oneshot::channel();
    let (resume, resumed) = oneshot::channel();
    pauses()
        .lock()
        .unwrap()
        .insert(channel, (captured, resumed));
    let old_producer = tokio::spawn({
        let state = state.clone();
        let tenant = tenant.clone();
        async move { emit_group_discovery_events(&tenant, &state, channel).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), did_capture)
        .await
        .expect("old producer reached capture")
        .expect("capture signalled");

    // The real edit path commits and publishes false while the older producer
    // still holds its true snapshot. No clock sleeps or scheduler luck needed.
    let edit = EventBuilder::new(Kind::Custom(9002), "")
        .tags([
            Tag::parse(["h", &channel.to_string()]).unwrap(),
            Tag::parse(["auto_join", "false"]).unwrap(),
        ])
        .sign_with_keys(&owner)
        .unwrap();
    let result = ingest_event(&state, &tenant, edit, http_auth(&owner))
        .await
        .unwrap();
    assert!(result.accepted, "{}", result.message);
    assert!(!has_auto_join(
        &group_state_tags(&state, &tenant, channel, 39000).await
    ));
    resume.send(()).expect("old producer waiting");
    old_producer.await.unwrap().unwrap();
    assert!(
        !has_auto_join(&group_state_tags(&state, &tenant, channel, 39000).await),
        "delayed discovery restored auto_join after the accepted false edit"
    );
}

//! The activation scope is one writer primary, encompassing every serving route.
//! Two-int advisory namespace BUAU/0 is distinct from bigint deletion/operation
//! locks. Order: scope -> operation advisory -> deletion -> community -> owners
//! -> issuer -> receipt -> boot/obligation. Never hold it across network/drain.
use crate::{Db, DbError, Result};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

/// Exact identity of the work that a relay has applied and fully drained.
/// This is an internal trusted-writer contract, never deserialized from HTTP.
#[derive(Clone, Copy, Debug)]
pub struct DrainAck {
    /// Boot incarnation that performed the drain.
    pub boot_id: Uuid,
    /// Operation whose participants were captured under the scope lock.
    pub operation_id: Uuid,
    /// Exact generation, not an implicit acknowledgement of older generations.
    pub policy_generation: i64,
}
impl DrainAck {
    /// Construct only after the local generation tracker and output owners drain.
    pub fn new(boot_id: Uuid, operation_id: Uuid, policy_generation: i64) -> Self {
        Self {
            boot_id,
            operation_id,
            policy_generation,
        }
    }
}

/// One pending durable target, used for startup and missed-notification recovery.
#[derive(Debug, sqlx::FromRow)]
pub struct ActivationTarget {
    /// Immutable target community, independent of host reuse.
    pub target_community_id: Uuid,
    /// Stable operator operation identity.
    pub operation_id: Uuid,
    /// Host fixed at acceptance; never supplied by a reconnecting client.
    pub expected_host: String,
    /// Issuer required by the accepted operation.
    pub expected_issuer: String,
    /// Exact required generation.
    pub policy_generation: i64,
    /// Acceptance time for pending-age observability.
    pub accepted_at: chrono::DateTime<chrono::Utc>,
}

pub(super) async fn lock(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query("SET LOCAL lock_timeout = '5s'")
        .execute(&mut **tx)
        .await?;
    sqlx::query("SET LOCAL statement_timeout = '15s'")
        .execute(&mut **tx)
        .await?;
    sqlx::query("SELECT pg_advisory_xact_lock(1112883541, 0)")
        .execute(&mut **tx)
        .await?; // 0x42554155, BUAU
    Ok(())
}

pub(super) async fn snapshot(
    tx: &mut Transaction<'_, Postgres>,
    operation: Uuid,
    generation: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO community_auth_obligations (operation_id, boot_id, policy_generation)
        SELECT $1, boot_id, $2 FROM community_auth_boots WHERE retired_at IS NULL
        ORDER BY boot_id ON CONFLICT DO NOTHING",
    )
    .bind(operation)
    .bind(generation)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

impl Db {
    /// Register immediately before serving, and return policy to install BEFORE
    /// admission. Registration and participant capture serialize on one primary.
    /// No heartbeat, timeout, replacement or readiness observation retires a boot.
    pub async fn register_auth_boot(
        &self,
        boot: Uuid,
        image: &str,
    ) -> Result<Vec<ActivationTarget>> {
        let mut tx = self.pool.begin().await?;
        lock(&mut tx).await?;
        sqlx::query(
            "INSERT INTO community_auth_baseline(singleton) VALUES(true) ON CONFLICT DO NOTHING",
        )
        .execute(&mut *tx)
        .await?;
        let inserted = sqlx::query(
            "INSERT INTO community_auth_boots(boot_id,image) VALUES($1,$2) ON CONFLICT DO NOTHING",
        )
        .bind(boot)
        .bind(image)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if inserted != 1 {
            return Err(DbError::AccessDenied("boot identity must be fresh".into()));
        }
        // A newly registered boot must install all requirements before serving.
        // Including it conservatively also prevents a registration/enable gap.
        sqlx::query(
            "INSERT INTO community_auth_obligations(operation_id,boot_id,policy_generation)
            SELECT operation_id,$1,policy_generation FROM community_auth_upgrades
            ORDER BY operation_id ON CONFLICT DO NOTHING",
        )
        .bind(boot)
        .execute(&mut *tx)
        .await?;
        let targets = sqlx::query_as("SELECT target_community_id,operation_id,expected_host,expected_issuer,policy_generation,accepted_at FROM community_auth_upgrades ORDER BY operation_id")
            .fetch_all(&mut *tx).await?;
        tx.commit().await?;
        Ok(targets)
    }

    /// Read unsatisfied obligations from the writer; notifications are hints only.
    pub async fn auth_activation_targets(&self, boot: Uuid) -> Result<Vec<ActivationTarget>> {
        Ok(sqlx::query_as("SELECT u.target_community_id,u.operation_id,u.expected_host,u.expected_issuer,u.policy_generation,u.accepted_at
            FROM community_auth_upgrades u JOIN community_auth_obligations o USING(operation_id)
            WHERE o.boot_id=$1 AND o.policy_generation=u.policy_generation AND o.satisfied_at IS NULL
            ORDER BY u.operation_id").bind(boot).fetch_all(&self.pool).await?)
    }

    /// Persist exact applied-and-drained evidence, refusing stale or foreign ACKs.
    /// Production completion cannot become active without verified baseline state.
    pub async fn acknowledge_auth_drain(&self, ack: DrainAck) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        lock(&mut tx).await?;
        lock_operation(self, &mut tx, ack.operation_id).await?;
        let updated=sqlx::query("UPDATE community_auth_obligations o SET satisfied_at=COALESCE(o.satisfied_at,now())
            WHERE o.operation_id=$1 AND o.boot_id=$2 AND o.policy_generation=$3
            AND EXISTS(SELECT 1 FROM community_auth_upgrades u WHERE u.operation_id=o.operation_id AND u.policy_generation=o.policy_generation)
            AND EXISTS(SELECT 1 FROM community_auth_boots b WHERE b.boot_id=o.boot_id AND b.retired_at IS NULL)")
            .bind(ack.operation_id).bind(ack.boot_id).bind(ack.policy_generation).execute(&mut *tx).await?.rows_affected();
        if updated == 1 {
            complete(&mut tx, ack.operation_id).await?;
        }
        tx.commit().await?;
        Ok(updated == 1)
    }

    /// Trusted self-retirement only, after irreversible admission stop and full
    /// process work drain. No HTTP/admin caller may discharge another boot.
    pub async fn retire_auth_boot(&self, boot: Uuid) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        lock(&mut tx).await?;
        let operations: Vec<Uuid> = sqlx::query_scalar("SELECT operation_id FROM community_auth_obligations WHERE boot_id=$1 ORDER BY operation_id")
            .bind(boot).fetch_all(&mut *tx).await?;
        for operation in &operations {
            lock_operation(self, &mut tx, *operation).await?;
        }
        let updated=sqlx::query("UPDATE community_auth_boots SET retired_at=COALESCE(retired_at,now()) WHERE boot_id=$1")
            .bind(boot).execute(&mut *tx).await?.rows_affected();
        sqlx::query("UPDATE community_auth_obligations SET satisfied_at=COALESCE(satisfied_at,now()) WHERE boot_id=$1")
            .bind(boot).execute(&mut *tx).await?;
        for operation in operations {
            complete(&mut tx, operation).await?;
        }
        tx.commit().await?;
        Ok(updated == 1)
    }

    /// Invalidate current completion under the same lock; historical timestamps
    /// remain intact. This does NOT clear required policy or restore enforcement.
    pub async fn invalidate_auth_baseline(&self) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        lock(&mut tx).await?;
        sqlx::query("UPDATE community_auth_baseline SET verified=false,epoch=epoch+1,invalidated_at=now() WHERE singleton")
            .execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}

async fn complete(tx: &mut Transaction<'_, Postgres>, operation: Uuid) -> Result<()> {
    // Eligibility remains authoritative in RECEIPT too. Never bless an empty
    // unregistered fleet; a verified baseline is an independent prerequisite.
    sqlx::query("UPDATE community_auth_upgrades u SET activated_at=COALESCE(u.activated_at,now()),activation_epoch=b.epoch,
        activation_history=CASE WHEN u.activation_epoch IS DISTINCT FROM b.epoch
            THEN u.activation_history || jsonb_build_array(jsonb_build_object('epoch',b.epoch,'at',now(),'baseline',b.evidence,
                'boots',(SELECT jsonb_agg(o.boot_id ORDER BY o.boot_id) FROM community_auth_obligations o WHERE o.operation_id=u.operation_id AND o.satisfied_at IS NOT NULL)))
            ELSE u.activation_history END
        FROM community_auth_baseline b WHERE b.singleton AND b.verified AND $2 AND u.operation_id=$1
        AND EXISTS(SELECT 1 FROM community_auth_obligations o WHERE o.operation_id=u.operation_id)
        AND NOT EXISTS(SELECT 1 FROM community_auth_obligations o WHERE o.operation_id=u.operation_id AND o.satisfied_at IS NULL)
        AND EXISTS(SELECT 1 FROM communities c WHERE c.id=u.target_community_id AND c.host=u.expected_host
            AND c.requires_nip_fi AND c.policy_generation=u.policy_generation AND c.archived_at IS NULL
            AND c.deleted_at IS NULL AND c.deletion_state='active'
            AND NOT EXISTS(SELECT 1 FROM community_deletion_requests d WHERE d.community_id=c.id AND d.stage<>'aborted')
            AND EXISTS(SELECT 1 FROM community_nip_fi_issuers i WHERE i.community_id=c.id AND i.issuer=u.expected_issuer))")
        .bind(operation).bind(cfg!(test)).execute(&mut **tx).await?;
    Ok(())
}

async fn lock_operation(
    db: &Db,
    tx: &mut Transaction<'_, Postgres>,
    operation: Uuid,
) -> Result<()> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,510))")
        .bind(operation.to_string())
        .execute(&mut **tx)
        .await?;
    let target: Option<Uuid> = sqlx::query_scalar(
        "SELECT target_community_id FROM community_auth_upgrades WHERE operation_id=$1",
    )
    .bind(operation)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(target) = target {
        let community = buzz_core::CommunityId::from_uuid(target);
        crate::deletion::lock_community_deletion_shared(tx, community).await?;
        sqlx::query("SELECT id FROM communities WHERE id=$1 FOR NO KEY UPDATE")
            .bind(target)
            .fetch_optional(&mut **tx)
            .await?;
        sqlx::query("SELECT pubkey FROM relay_members WHERE community_id=$1 AND role='owner' ORDER BY pubkey FOR UPDATE").bind(target).fetch_all(&mut **tx).await?;
        sqlx::query("SELECT issuer FROM community_nip_fi_issuers WHERE community_id=$1 ORDER BY issuer FOR UPDATE").bind(target).fetch_all(&mut **tx).await?;
        // Ineligible/deleted operations still retain their evidence, but never activate.
        // No serving mutation is performed here, so do not require active admission.
        let _ = db;
    }
    sqlx::query(
        "SELECT operation_id FROM community_auth_upgrades WHERE operation_id=$1 FOR UPDATE",
    )
    .bind(operation)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(())
}

// This constructor and all active-state authority tests live inside buzz-db's
// own unit-test modules. It is absent from production and dependency builds.
#[cfg(test)]
pub(super) async fn verify_baseline_for_test(db: &Db) {
    let mut tx = db.pool.begin().await.unwrap();
    lock(&mut tx).await.unwrap();
    sqlx::query("UPDATE community_auth_baseline SET verified=true,epoch=epoch+1,evidence='{\"test_only\":true}' WHERE singleton")
        .execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
}

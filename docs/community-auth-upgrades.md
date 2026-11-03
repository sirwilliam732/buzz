# Community NIP-FI upgrade operator contract

BUZZ-510 adds durable policy acceptance. BUZZ-511 supplies per-community
request enforcement and discovery; BUZZ-512 supplies existing-session activation.
Do not expose upgrade calls in a deployment until those releases and the serving
fleet baseline have been verified. This change alone cannot establish enforcement.
It never reports `active` and never substitutes a timer for activation evidence.

The operator HTTP plane is separate from tenant event ingest. These management
operations extend the existing provisioning API; community clients cannot invoke
them with ordinary membership credentials. Both routes require an allowlisted
operator's NIP-98 signature bound to the configured operator API origin, exact
method and URL, including the query string on GET, and the POST body hash.
Operator POST verification does not bind query strings. Each attempt needs a fresh
signature; operation idempotency does not bypass transport replay protection.
Responses use `Cache-Control: no-store`.

`BUZZ_NIP_FI_UPGRADES_ENABLED` defaults to `false`. While disabled, new
acceptances return 503 `authorization_unavailable` with no policy mutation or
receipt. Authorized GET and identical POST receipt recovery remain available;
principal checks and payload conflicts are unchanged. Enabling this deployment
switch is an explicit operator action after the enforcement and activation fleet
baseline is verified; caller release discipline alone cannot enable acceptance.

## Generation storage

`communities.policy_generation` is `BIGINT NOT NULL DEFAULT 0`, without a
nonnegative database CHECK. Supported writers retain the current generation
when the community already requires FI, or increment it by one on acceptance.
Starting from zero, those writes preserve the nonnegative invariant. Direct SQL
can insert a negative community generation and is outside that writer contract.
Positive-generation CHECKs on acceptance receipts remain in place; activation
and receipt matching still require the exact accepted generation.

The community CHECK is omitted to avoid validating existing rows while adding
the column. The current schema planner folds a column and its CHECK together,
and can rebuild and validate a CHECK when its validation metadata differs.
Migration, fresh desired-state creation and later reconciliation therefore all
omit this community CHECK. No bootstrap or schema-tooling workaround is required.

## Enable

`POST /operator/communities/nip-fi-upgrade`

```json
{
  "operation_id": "c2ffcc20-9505-48fd-b6c7-446aa833bcd1",
  "community_id": "c9acf9a6-37d5-4af1-8df9-92a165637b01",
  "expected_host": "team.communities.example",
  "actor_pubkey": "<owner's registered 64-character hex public key>",
  "expected_owner_pubkey": "<expected current owner's hex public key>",
  "expected_issuer": "https://identity.example/api/goose"
}
```

The trusted operator mediates the authenticated actor. Kgoose must resolve that
actor's usable registered key from its session; it must never accept a user-supplied
actor key as identity. The actor must equal the expected and current owner.
Host and keys normalize before comparison; issuer bytes match exactly. UUIDs
must be non-nil. Unknown JSON fields are rejected.

The deployment selects the expected issuer from the **single** authorized issuer
in `BUZZ_NIP_FI_PROVISION_BINDING`. An absent or ambiguous selection is unavailable.
The target must be bindable under that policy and the issuer must have a configured
`BUZZ_NIP_FI_ISSUERS` policy. The same issuer must already be bound to the community
in `community_nip_fi_issuers`. The call never adds or broadens trust.

A transaction serializes UUID reuse across targets, then community lifecycle,
host and owner checks against transfer/archive/delete. It locks the exact issuer
row against unbind. Only a first optional-to-required transition increments
`policy_generation`; concurrent distinct operations converge on that generation.
Acceptance and its receipt commit together. No timeout or retry clears the policy.

## Readback and response

`GET /operator/communities/nip-fi-upgrade?operation_id=<UUID>`

Only the original authenticated operator can recover the receipt. Readback
survives owner transfer, archive and deletion; the recorded owner is historical
acceptance evidence, not authority for a fresh enable.

Enable and readback return HTTP 200 for an accepted receipt:

```json
{
  "operation_id": "c2ffcc20-9505-48fd-b6c7-446aa833bcd1",
  "community_id": "c9acf9a6-37d5-4af1-8df9-92a165637b01",
  "expected_host": "team.communities.example",
  "actor_pubkey": "<owner at acceptance>",
  "expected_owner_pubkey": "<expected owner at acceptance>",
  "expected_issuer": "https://identity.example/api/goose",
  "operator_pubkey": "<authenticated operator>",
  "policy_generation": 1,
  "requires_nip_fi": true,
  "state": "pending",
  "pending_reasons": ["registry_baseline_unverified"]
}
```

`requires_nip_fi: true` records durable acceptance. `state` reports availability
and activation separately. BUZZ-510 returns `pending` while the target's policy,
lifecycle and issuer binding still match, and `unavailable` if they do not or
trusted issuer configuration is no longer applicable. A missing database response
is HTTP 503, never a fabricated receipt. A receipt is not proof of current service
health. No response in this version reports `active`.

`pending_reasons` applies only while `state` is `pending`; clients must ignore
those obligations when state is `unavailable`, which always takes precedence.
`pending_reasons` is extensible. Clients must preserve pending on unknown values.
`registry_baseline_unverified` prevents completion until BUZZ-512 verifies fleet
membership. That ticket may add `boot_application_pending`, `boot_drain_pending`
and `boot_unreachable`, and must bind activation evidence to the exact operation
and generation. Baseline invalidation must prevent `active`.

The UUID binds the normalized community/host/actor/owner/issuer/operator tuple.
Identical retries recover that tuple and generation before rechecking mutable
preconditions. Changed payload or principal conflicts. After a lost POST response,
read by UUID or retry the same payload with a fresh NIP-98 signature. Do not generate
a new UUID to hide an ambiguous response. No rollback endpoint exists.

| HTTP | `code` | Meaning |
|---|---|---|
| 400 | `invalid_request` | Invalid enable JSON, UUID, host or key |
| 401/403 | Existing operator auth errors | Missing/invalid credentials or operator not allowlisted |
| 404 | `community_not_found` | No target for a first acceptance |
| 404 | `operation_not_found` | No receipt owned by this operator |
| 409 | `idempotency_conflict` | UUID already bound to a different tuple/principal |
| 409 | `host_conflict` | ID does not map to expected host |
| 409 | `owner_conflict` | Actor/expected owner/current owner disagree |
| 409 | `lifecycle_conflict` | Archive/deletion or durable deletion intent |
| 409 | `issuer_conflict` | Expected issuer/host not approved, or exact stored binding absent |
| 503 | `authorization_unavailable` | Trusted config or authoritative DB unavailable; retry/read back same UUID |

Malformed GET queries use Axum's existing query-extractor rejection. Transport
signature replay uses the existing operator replay error independently of this table.

## Schema and rollout

Migration 0082 is additive: legacy rows keep `requires_nip_fi=false` and generation
zero. It adds no trigger, function, procedure, rule or foreign key and rewrites no
existing rows. Lock and statement budgets are 5 and 30 seconds. Existing global
`enforce` remains an additional floor; this ticket changes no data-plane gate.
Do not infer requirements from issuer rows, backfill free communities, or remove
the enterprise enforce floor as part of this migration.

Apply schema before new code. Desired-state and migration definitions match;
The registry entry is static lint metadata, so it is present in the migration
and `schema/seeds/`, not the pgschema reconciliation script (database.md Rule 3).
This follows the other operator-global registry entries. Receipts intentionally survive community purge:
the target ID/host/owner tuple prevents reusing a lost-response UUID for another
community, including after deletion. They are operator-only recovery evidence.
No tenant serving writer owns this table and no fence trigger is attached.

The active-community primary accessor is
`Db::community_auth_policy(CommunityId) -> Result<Option<CommunityAuthPolicy>>`.
`None` means no active target; errors must fail closed. BUZZ-511 must combine
its stored requirement with global enforce and honor repair denial first.

## Mistaken acceptance and containment

Stop the initiating caller and set `BUZZ_NIP_FI_UPGRADES_ENABLED=false` to prevent
new acceptances. Keep authenticated status/readback available. Record the exact
operation, target and generation for operator-led incident assessment and recovery.
Preserve the durable requirement, issuer trust and receipts while restoring a
working FI path or containing the affected community through the established
archive/lifecycle controls. Coordinate any necessary issuer/configuration repair
through its existing operator procedure; never broaden trust to make a probe pass.

Disabling calls or rolling back code does not undo acceptance. Never restore a
serving binary that ignores the durable requirement; retain effective enforcement
or contain the community while repairing the FI path. There is no
clear-policy API or routine SQL reset, and neither a timeout nor a mistaken call
is permission to return the community to free admission. Recovery must retain
the accepted requirement and operation evidence. No fleet-wide drain or deployment
is an automatic fallback.

## Request enforcement and discovery (BUZZ-511)

Each protected request resolves the normalized Host's active policy from the
primary database, with a 500 ms budget. The effective requirement is the stored
`requires_nip_fi` OR global `enforce`. Global `deny_protected` always returns 503
before evidence checks; neither `off` nor `shadow` exempts a required row.
Existing Nostr proof, key pairing, deny map, membership and resource checks remain.
Discovery, pairing, enrollment and operator exceptions keep their own authentication.

The router shares the policy read with HTTP handlers for that request. Root and
audio WebSocket admission use the same policy source. There is no cross-request
policy cache, positive or negative: an older read cannot overwrite a newer cached
generation. This adds one writer query per protected request or discovery request;
`buzz_nip_fi_policy_reads_total` and `buzz_nip_fi_policy_read_seconds` measure it.
Protected HTTP handlers use the immutable tenant ID from the same primary row
that supplied the request's policy. They do not resolve tenant-scoped data from
another host-cache snapshot. NIP-11 uses that policy tenant for its descriptors
and optional decorations too, so a remapped host cannot combine one community's
optional policy with another community's data. Exempt and unguarded routes keep
their existing host-cache behavior.

Issuer-binding/JWKS caches keep their existing bounds; unavailable dependencies
refuse required admission, rather than treating it as optional.

WebSocket upgrades also bind tenant identity and its policy from the same primary
row. An earlier missing or optional policy result cannot authorize a socket for a
required row that becomes serving during lookup (for example, on unarchive).
Before upgrade, enforce that bound row's requirement even when the early check
ran in Off or Shadow. This changes no discovery fields.

Database-sourced deployments construct the issuer resolver and verifier even in
`off`, because some rows may require FI. Configure finite
`BUZZ_NIP_FI_MAX_CONNECTION_LIFETIME_SECS`, issuer JWKS and command configuration
before enabling any row. Missing required components return 503. An optional row
still follows global off/shadow behavior, including signed and unsigned legacy
proofs where the endpoint permits them.

Both `GET /info` and root NIP-11 negotiation return `Cache-Control: no-store`.
An unknown or non-serving Host returns generic 404; policy lookup failure returns
503. The top-level `requires_nip_fi` is a host-specific boolean. Existing
`limitation.federated_identity` and `federated_identity` descriptor fields retain
capability semantics: advertise when global mode evaluates OR the host requires
FI. Thus shadow optional hosts still advertise capability, and required off hosts
advertise capability and requirement together. No issuer, audience or identity
provider address is added to discovery. Existing clients can keep reading their
current fields; new clients must use the requirement field to distinguish optional
capability from mandatory evidence.

This is admission enforcement only. Existing requests and sockets may predate the
new policy. Only BUZZ-512's application and drain evidence may establish completed
activation; the operator receipt remains pending until that evidence is recorded.

### Staging and containment

This changes every serving host, including free hosts: protected routes and
discovery now require a primary policy read. Unmapped discovery previously could
return a generic document; it now returns 404. Policy failures return 503.
Database-source startup loads its binding snapshot even in Off and fails startup
if that load fails. The local dependency gauge
`buzz_nip_fi_required_admission_ready` must be 1 before accepting any upgrade;
it covers verifier, deny map and finite lifetime, not fleet baseline verification.
Binding and JWKS readiness must also be verified through a signed request.

Keep `BUZZ_NIP_FI_UPGRADES_ENABLED=false` during the initial staging rollout.
Start with zero required rows and record a 30-minute baseline using representative
free signed/unsigned HTTP, root/audio WS and discovery traffic. Compare identical
traffic before and after this change. Record request rate, policy-read p95/p99,
writer pool acquisition p99 (`buzz_db_pool_acquire_duration_seconds`,
`pool_role=writer`, `operation=authorization`), policy timeout/error count, HTTP
503 rate and startup/restart success. A local PostgreSQL 16 probe measured 100
serial reads in 13.36 ms; that is not a production capacity result.

Initial staging abort criteria (validate before increasing traffic): any policy
timeout, policy-induced 503, unexpected free-admission denial or startup failure;
policy-read p99 above 50 ms; writer acquisition p99 above 20 ms or more than twice
its pre-change baseline; total 503 rate more than 0.1 percentage point above
baseline over five minutes. These are conservative rollout limits, not measured
production guarantees. Do not expand until the full 30-minute run stays within
them and the operator has reviewed measured results. Verify unknown-host 404 and
dependency-failure 503 with no-store explicitly.

Then test a disposable required community with complete FI dependencies, in both
Off and Shadow: unsigned/missing evidence denies, signed valid FI succeeds,
root/audio upgrades agree, discovery agrees, and a pending deletion request does
not weaken the requirement. Before production acceptance, complete BUZZ-512's
session application/drain and the production baseline-verifier gate. No such
production verification is established by this PR.

Before any required row exists, an operator may stop the rollout and restore the
previous binary after verifying the primary contains zero required rows. Once a
requirement has been accepted, a pre-511 binary is unsafe: it ignores that row.
Stop new upgrade calls, retain policy and receipt records, and keep an enforcing
binary while repairing dependencies. If admission cannot be restored, contain the
affected serving hosts with an operator-approved repair-mode/traffic action;
never change them to Off as a bypass or clear accepted policy. No automatic fleet
drain or deployment is prescribed here. Existing receipt recovery remains usable
while new acceptances are disabled.

Required sessions admitted under global Shadow are real enforced FI sessions.
BUZZ-512 closes these enforced sessions independently of global rollout mode,
while preserving the separate optional-shadow registry. Do not enable production
upgrades until the activation/control behavior and baseline authority are verified.

## Activation tracking (BUZZ-512 draft)

Policy acceptance and completed enforcement are different states. The relay
registers a fresh boot UUID on its writer primary before public listener binding.
Enable captures every non-retired registered boot under the same new two-integer
advisory lock (`0x42554155`, `0`). All routes in the coordination scope must use
that primary, including canaries and other regions. Another primary cannot join
this scope. The initial implementation uses the entire writer database as its
scope rather than a Kubernetes Deployment name.

A five-second durable reconciliation installs sticky community-generation floors.
The tracker keys work by immutable community ID, so a reused host does not inherit
another community's floor. Reconciliation
cancels older work, and retries exact boot/operation/generation acknowledgements.
The existing cluster cache-invalidation bus carries a policy-snapshot wake hint.
Receivers re-read the writer; hints contain no authority and never cancel sockets
directly. Lost hints fall back to the five-second poll and cannot discharge an obligation.
Delayed policy reads cannot overwrite an installed floor. New valid FI work at
the required generation is counted separately. Earlier enterprise sessions may
need to reconnect even if they previously supplied a valid assertion.

Root frame handlers are joined before subscription cleanup. Audio teardown joins
its writer, forwarder and control reader. HTTP response bodies and asynchronous
producers retain work tickets through completion, including event-triggered
workflow runs and queued audit appends. Cancellation counts, heartbeat
expiry, readiness removal and replacement boots do not prove completion. The
client need not consume bytes already sent before the fence.

The new tables are operator-global evidence and remain after target deletion.
They add no triggers, functions or foreign keys. Migration 0082 is additive:
old relays ignore the new tables and nullable receipt columns. Desired-state
and migrated catalogs are compared in the existing PostgreSQL parity test.

### Production activation is not yet available

**This draft does not satisfy BUZZ-512's production-active acceptance.** A trusted
baseline verifier remains unfinished scope of that same ticket. Production
builds hard-disable the authority input to completion/readback. Only buzz-db's
own `cfg(test)` modules can exercise verified-baseline active transitions.
There is no public arm, external discharge, force-active, reset or clear API.
Do not set the baseline through operational SQL as a substitute for verification.

A baseline authority must prove the complete serving-route inventory, compatible
images/capabilities, and absence of every old or dark-mode nonparticipant and
its work. It must enforce admission and routing restrictions against reentry.
An evidence-kind string, caller assertion, missing pod or signed inventory
snapshot alone is insufficient. No such authority is included here.

Before an eventual baseline is armed, all serving routes must be covered.
After arming, a new process must participate before serving *any* host, including
free hosts, or durably invalidate the epoch under the scope lock before serving.
Without writer access it cannot do either. This adds startup/scale-out
availability cost. Already-participating free sessions preserve existing outage
behavior; there are no periodic registration leases or automatic fleet drains.
The first draft's startup fallback is dark-only and cannot establish a baseline.

Baseline invalidation makes otherwise eligible operations pending; lifecycle,
required-policy or issuer-trust loss still wins as unavailable. Historical
activation time and append-only per-epoch evidence (including the baseline and
participating boots) are retained separately from current eligibility. Required policy
and local floors never clear because a baseline invalidates. A rollback must not
serve required hosts with pre-511 code. If no compatible rollback exists, stop
serving those hosts. No rollout or migration execution is authorized by this PR.

A boot may self-retire only after admission irreversibly stops and owned work
has drained. A hard stop or unresolved work leaves obligations pending. External
death recovery remains unsupported until a concrete trust source and verifier
can prove that an incarnation cannot resume; a new pod cannot retire its
predecessor. Preserve the task database and evidence while investigating.

Required communities admitted under global Shadow use real FI sessions. Signed
administrative disconnect commands now close matching enforced sessions locally
and across pods independently of the global rollout mode. Optional shadow
observations remain in their separate observation registry.

## Tracking rollout and abort criteria (BUZZ-512 partial draft)

`BUZZ_NIP_FI_ACTIVATION_TRACKING_ENABLED=false` is the default pre-arming
setting. With both tracking and upgrade acceptance disabled, a writer scope with
no operations, `verified=false`, and baseline epoch zero may skip boot registration,
work tickets, per-frame tracker locks and detached HTTP ownership. Startup still
checks that condition under the writer scope lock; a writer error prevents the
public listener from opening. Enabling upgrade acceptance also opts the boot into
tracking. The registered boot remains tracked for its whole process lifetime.

Any operation, a positive baseline epoch, or a verified baseline forces startup
registration regardless of this setting. Epochs only advance, including on
invalidation; invalidating a baseline cannot restore bypass. Registration and the
bypass decision serialize with enable and baseline changes. No registration error
falls back to unregistered public serving. Registered boots keep reconciling and
draining even if operators later disable acceptance or change deployment settings.
A replacement boot installs all durable requirements before listening. The
reconciler only ACKs its own unsatisfied obligations; already satisfied operations
cause no repeated ACK transaction while production completion is disabled.

A **dark bypass boot is not a verified participant**. There is no production
arming endpoint in this draft. A future trusted arming authority must establish
that every route is covered by registered tracking-capable boots and that every
dark bypass boot has permanently stopped serving and cannot reenter before
advancing the epoch. A config rollout, registry list, or readiness observation is
insufficient. Before enabling upgrade acceptance, stop/restart every bypass boot into
registered tracking; do not accept the first operation while any bypass boot
can still serve. A dark boot does not discover an arming transition in the
background: the missing authority must establish permanent exclusion of every
such incarnation. Starting this build with tracking disabled is not permission
to arm a mixed fleet. Dark opt-in boots that crash remain durable participants and become
pending obligations at enable; they cannot disappear at first arming. Likewise,
boots which predate registration need trusted inventory and non-serving/no-reentry
evidence. Any exclusion/discharge must record that evidence under the scope lock,
invalidate the epoch, and reverify coverage. Age, missing heartbeats, registry
absence, replaced pods, and inferred node death are never evidence of retirement.
**Production `active` remains disabled until that authority is implemented and
reviewed in BUZZ-512.**

While tracking is on, protected HTTP handlers run in owned Tokio tasks. Client
disconnect does not cancel an admitted handler or its in-flight DB effects.
Response producers, queued audit entries and workflow descendants retain their
work ticket through cleanup; cancelled generations cannot deliver more protected
response frames. A handler panic is logged and maps to 503
`authorization_unavailable` when a response is still possible. This differs from
untracked requests, which run in their caller's future. Each ticket enter/drop and
protected response-frame poll takes a process-wide mutex; compliant traffic can
contend with floor installation. The manual `activation_tracking_mutex_cost`
microbenchmark measures uncontended and four-thread enter/drop plus empty body
poll cost. These synthetic costs do not establish production tail latency or
throughput; load-test the actual HTTP, root and audio mix before arming.

Roll out dark tracking to a small controlled cohort first, retaining upgrade
acceptance off. Compare authoritative serving inventory against registered boot
IDs, images and `serving_at`; registration is before public bind, so the registry
alone overcounts rather than proving serving coverage. Monitor
`buzz_nip_fi_activation_tracking_enabled`,
`buzz_nip_fi_activation_registration_success_total`,
`buzz_nip_fi_activation_retirement_attempts_total`,
`buzz_nip_fi_activation_retirement_success_total`, local obligation counts/oldest
pending age, scope-lock timeout logs, DB pool wait, request latency, CPU and
memory against the same workload on bypass boots. Retirement success is meaningful
only for observed graceful exits; OOM/SIGKILL and drain-timeout boots intentionally
remain pending. Both TCP and UDS must stop accepting and join, followed by all
socket/handler/body/audit tickets, before a boot persists retirement. The existing
hard-exit backstop remains armed throughout; it never converts an unknown drain
to retirement.

Abort progression on any serving boot without registration in the proposed armed
coverage, unexpected registration failure, graceful exit without retirement,
unexplained pending growth, scope-lock/pool pressure, lost policy closes, or a
material request latency/throughput regression against the cohort baseline.
Before arming, stop expanding the cohort and disable acceptance; a bypass rollout
requires trusted confirmation that no operation/armed epoch exists. After arming,
disable **new acceptance only**, retain required-policy enforcement, reconciliation
and drains, and repair/retry pending work. Do not reset epochs, delete obligations,
turn off tracking for serving participants, or drain the fleet automatically.
Generation cancellation and post-upgrade refusal use Close 1008 with reason
`community authentication upgraded`; reconnect with a fresh, valid required
assertion. Handshake-time refusals retain their HTTP denial behavior.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Writer-primary-wide activation scope. No production verifier can arm this yet.
CREATE TABLE community_auth_baseline (
    singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    epoch BIGINT NOT NULL DEFAULT 0,
    verified BOOLEAN NOT NULL DEFAULT false,
    evidence JSONB NOT NULL DEFAULT '{}',
    invalidated_at TIMESTAMPTZ
);

-- Operator-global serving incarnations. Retained to prevent restart erasing obligations.
CREATE TABLE community_auth_boots (
    boot_id UUID PRIMARY KEY,
    image TEXT NOT NULL,
    serving_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    retired_at TIMESTAMPTZ
);

-- Operator-global operation/boot evidence retained even after target deletion.
CREATE TABLE community_auth_obligations (
    operation_id UUID NOT NULL,
    boot_id UUID NOT NULL,
    policy_generation BIGINT NOT NULL,
    satisfied_at TIMESTAMPTZ,
    PRIMARY KEY (operation_id, boot_id, policy_generation)
);

CREATE INDEX community_auth_obligations_boot_idx ON community_auth_obligations (boot_id, operation_id);

ALTER TABLE community_auth_upgrades ADD COLUMN activated_at TIMESTAMPTZ, ADD COLUMN activation_epoch BIGINT, ADD COLUMN activation_history JSONB NOT NULL DEFAULT '[]';
INSERT INTO _operator_global_tables (table_name, reason) VALUES
    ('community_auth_baseline', 'activation evidence retained across target deletion'),
    ('community_auth_boots', 'activation evidence retained across target deletion'),
    ('community_auth_obligations', 'activation evidence retained across target deletion');

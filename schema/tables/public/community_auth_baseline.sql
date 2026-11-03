-- Writer-primary-wide activation scope. No production verifier can arm this yet.
CREATE TABLE community_auth_baseline (
    singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    epoch BIGINT NOT NULL DEFAULT 0,
    verified BOOLEAN NOT NULL DEFAULT false,
    evidence JSONB NOT NULL DEFAULT '{}',
    invalidated_at TIMESTAMPTZ
);

-- Operator-global operation/boot evidence retained even after target deletion.
CREATE TABLE community_auth_obligations (
    operation_id UUID NOT NULL,
    boot_id UUID NOT NULL,
    policy_generation BIGINT NOT NULL,
    satisfied_at TIMESTAMPTZ,
    PRIMARY KEY (operation_id, boot_id, policy_generation)
);
CREATE INDEX community_auth_obligations_boot_idx ON community_auth_obligations (boot_id, operation_id);

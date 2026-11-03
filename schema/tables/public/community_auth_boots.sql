-- Operator-global serving incarnations. Retained to prevent restart erasing obligations.
CREATE TABLE community_auth_boots (
    boot_id UUID PRIMARY KEY,
    image TEXT NOT NULL,
    serving_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    retired_at TIMESTAMPTZ
);

BEGIN;

ALTER TABLE reconciliation_issues
    ADD COLUMN remote_block_number BIGINT NOT NULL DEFAULT 0,
    ADD CONSTRAINT reconciliation_issues_remote_block_number_nonnegative
        CHECK (remote_block_number >= 0);

COMMIT;

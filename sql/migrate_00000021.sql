
CREATE TYPE transaction_status AS ENUM ('new', 'queued', 'unconfirmed', 'confirmed', 'error');

ALTER TABLE transactions
  ALTER COLUMN status TYPE transaction_status USING status::transaction_status;
ALTER TABLE transactions
  ALTER COLUMN status SET DEFAULT 'new';

UPDATE transactions SET status = 'new' WHERE status IS NULL;

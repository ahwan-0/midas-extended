CREATE TABLE user_record (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    balance NUMERIC(19, 4) NOT NULL DEFAULT 0
);

CREATE TABLE transaction_record (
    id BIGSERIAL PRIMARY KEY,
    sender_id BIGINT REFERENCES user_record(id),
    recipient_id BIGINT REFERENCES user_record(id),
    amount NUMERIC(19, 4) NOT NULL,
    incentive NUMERIC(19, 4) NOT NULL DEFAULT 0,
    timestamp TIMESTAMP NOT NULL
);

CREATE INDEX idx_transaction_sender ON transaction_record(sender_id);
CREATE INDEX idx_transaction_recipient ON transaction_record(recipient_id);
CREATE INDEX idx_transaction_timestamp ON transaction_record(timestamp);
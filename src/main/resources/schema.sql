DROP TABLE IF EXISTS transaction_table;
CREATE TABLE transaction_table (
    transaction_id  NUMBER,
    currency        VARCHAR(3),
    value           VARCHAR(10)
);
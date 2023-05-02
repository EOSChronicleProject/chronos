
CREATE INDEX blocks_id ON blocks (block_id);

CREATE MATERIALIZED VIEW block_by_date AS
    SELECT *
    FROM blocks
    WHERE block_num IS NOT NULL AND block_date IS NOT NULL
    PRIMARY KEY (block_date, block_num);



CREATE INDEX transactions_trx_id ON transactions (trx_id);
CREATE INDEX transactions_seq ON transactions (seq);



CREATE MATERIALIZED VIEW account_receipts AS
    SELECT *
    FROM receipts
    WHERE block_num IS NOT NULL AND receiver IS NOT NULL AND
          recv_sequence IS NOT NULL AND contract IS NOT NULL AND action IS NOT NULL AND block_date IS NOT NULL
    PRIMARY KEY ((receiver, block_date), recv_sequence, contract, action, block_num);


CREATE MATERIALIZED VIEW action_receipts AS
    SELECT *
    FROM receipts
    WHERE block_num IS NOT NULL AND receiver IS NOT NULL AND
          recv_sequence IS NOT NULL AND contract IS NOT NULL AND action IS NOT NULL AND block_date IS NOT NULL
    PRIMARY KEY ((receiver, contract, action, block_date), recv_sequence, block_num);






CREATE MATERIALIZED VIEW abi_by_account AS
    SELECT contract, block_num, abi_raw
    FROM abi_history
    WHERE contract IS NOT NULL
    PRIMARY KEY (contract, block_num)
    WITH CLUSTERING ORDER BY (block_num DESC);

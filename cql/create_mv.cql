
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
    WHERE account_name IS NOT NULL AND block_date IS NOT NULL AND recv_sequence_start IS NOT NULL AND seq IS NOT NULL
    PRIMARY KEY ((account_name, block_date), recv_sequence_start, seq, block_num);




CREATE MATERIALIZED VIEW contract_actions AS
    SELECT *
    FROM actions
    WHERE  contract IS NOT NULL AND block_date IS NOT NULL AND action IS NOT NULL AND seq IS NOT NULL
    PRIMARY KEY ((contract, block_date), action, block_num, seq);


CREATE MATERIALIZED VIEW abi_by_account AS
    SELECT account_name, block_num, abi_raw
    FROM abi_history
    WHERE account_name IS NOT NULL
    PRIMARY KEY (account_name, block_num)
    WITH CLUSTERING ORDER BY (block_num DESC);

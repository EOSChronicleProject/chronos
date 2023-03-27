/*
   id=0: last written block,
   id=1: irreversible block,
   id=2: lowest block in history,
*/
CREATE TABLE pointers
(
 id INT PRIMARY KEY,
 ptr BIGINT
);


CREATE TABLE transactions
(
 block_num         BIGINT,
 block_time        TIMESTAMP,
 seq               BIGINT,
 trx_id            BLOB,
 trace             BLOB,
 PRIMARY KEY (block_num, seq)
);

CREATE INDEX transactions_trx_id ON transactions (trx_id);


CREATE TABLE receipts
(
 block_num              BIGINT,
 block_time             TIMESTAMP,
 seq                    BIGINT,
 account_name           ASCII,
 recv_sequence_start    BIGINT,
 recv_sequence_count    BIGINT,
 PRIMARY KEY (block_num, seq, account_name) 
);


CREATE MATERIALIZED VIEW account_receipts AS
    SELECT  block_num, block_time, seq, account_name, recv_sequence_start, recv_sequence_count
    FROM receipts
    WHERE account_name IS NOT NULL AND recv_sequence_start IS NOT NULL AND seq IS NOT NULL
    PRIMARY KEY (account_name, recv_sequence_start, seq, block_num);

CREATE MATERIALIZED VIEW account_receipts_by_time AS
    SELECT  block_num, block_time, seq, account_name, recv_sequence_start, recv_sequence_count
    FROM receipts
    WHERE account_name IS NOT NULL AND block_time IS NOT NULL AND seq IS NOT NULL
    PRIMARY KEY (account_name, block_time, seq, block_num);



CREATE TABLE recv_sequence_max
(
 account_name           ASCII,
 recv_sequence_max      BIGINT,
 PRIMARY KEY (account_name)
);


CREATE TABLE actions
(
 block_num         BIGINT,
 block_time        TIMESTAMP,
 seq               BIGINT,
 contract          ASCII,
 action            ASCII,
 PRIMARY KEY (block_num, seq, contract, action) 
);


CREATE MATERIALIZED VIEW contract_actions AS
    SELECT  block_num, block_time, seq, contract, action
    FROM actions
    WHERE  contract IS NOT NULL AND action IS NOT NULL AND seq IS NOT NULL
    PRIMARY KEY (contract, action, block_num, seq);





CREATE TABLE abi_history
(
 account_name           ASCII,
 block_num              BIGINT,
 abi_raw                BLOB,
 PRIMARY KEY (account_name, block_num)
) WITH CLUSTERING ORDER BY (block_num DESC);

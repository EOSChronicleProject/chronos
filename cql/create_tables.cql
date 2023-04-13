/* primary keys in all history tables use block_num as the partition
   key because the rows need to be deleted quickly on forks */


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


CREATE TABLE blocks
(
 block_num         BIGINT,
 block_time        TIMESTAMP,
 block_id          BLOB,
 producer          ASCII,
 previous          BLOB,
 transaction_mroot BLOB,
 action_mroot      BLOB,
 trx_count         INT,
 PRIMARY KEY (block_num)
);

CREATE INDEX blocks_id ON blocks (block_id);


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
 block_date             BIGINT,
 seq                    BIGINT,
 account_name           ASCII,
 recv_sequence_start    BIGINT,
 recv_sequence_count    BIGINT,
 PRIMARY KEY (block_num, seq, account_name, block_date) 
);


CREATE MATERIALIZED VIEW account_receipts AS
    SELECT *    
    FROM receipts
    WHERE account_name IS NOT NULL AND block_date IS NOT NULL AND recv_sequence_start IS NOT NULL AND seq IS NOT NULL
    PRIMARY KEY ((account_name, block_date), recv_sequence_start, seq, block_num);


CREATE TABLE receipt_dates
(
 account_name           ASCII,
 block_date             BIGINT,
 PRIMARY KEY (account_name, block_date)
);



CREATE TABLE actions
(
 block_num         BIGINT,
 block_time        TIMESTAMP,
 block_date        BIGINT,
 seq               BIGINT,
 contract          ASCII,
 action            ASCII,
 PRIMARY KEY (block_num, seq, contract, action) 
);


CREATE MATERIALIZED VIEW contract_actions AS
    SELECT *
    FROM actions
    WHERE  contract IS NOT NULL AND block_date IS NOT NULL AND action IS NOT NULL AND seq IS NOT NULL
    PRIMARY KEY ((contract, block_date), action, block_num, seq);


CREATE TABLE action_dates
(
 contract               ASCII,
 action                 ASCII,
 block_date             BIGINT,
 PRIMARY KEY ((contract, action), block_date)
);





CREATE TABLE abi_history
(
 block_num              BIGINT,
 account_name           ASCII,
 abi_raw                BLOB,
 PRIMARY KEY (block_num, account_name)
);

CREATE MATERIALIZED VIEW abi_by_account AS
    SELECT account_name, block_num, abi_raw
    FROM abi_history
    WHERE account_name IS NOT NULL
    PRIMARY KEY (account_name, block_num)
    WITH CLUSTERING ORDER BY (block_num DESC);

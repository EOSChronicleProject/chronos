/* primary keys in all history tables use block_num as the partition
   key because the rows need to be deleted quickly on forks */


/*
   id=0: last written block,
   id=1: irreversible block,
   id=2: lowest block in history,
   id=3: lowest block where traces are populated
   if=4: lowest block for traces scheduled for deletion
*/
CREATE TABLE pointers
(
 id INT PRIMARY KEY,
 ptr BIGINT
);


/* block attributes.
   block_date is a start of a 24h day, in milliseconds, in UTC zone */
CREATE TABLE blocks
(
 block_num         BIGINT,
 block_time        TIMESTAMP,
 block_date        BIGINT,
 block_id          BLOB,
 producer          ASCII,
 previous          BLOB,
 transaction_mroot BLOB,
 action_mroot      BLOB,
 trx_count         INT,
 PRIMARY KEY (block_num)
);

CREATE INDEX blocks_id ON blocks (block_id);

CREATE MATERIALIZED VIEW block_by_date AS
    SELECT *    
    FROM blocks
    WHERE block_num IS NOT NULL AND block_date IS NOT NULL
    PRIMARY KEY (block_date, block_num);


/* transaction index.
   seq is the first global sequence number of the first action receipt.
   trx_id is a 256-bit byte array */
CREATE TABLE transactions
(
 block_num         BIGINT,
 block_time        TIMESTAMP,
 seq               BIGINT,
 block_pos         INT,     /* position inside the block */
 trx_id            BLOB,
 PRIMARY KEY (block_num, seq)
);

CREATE INDEX transactions_trx_id ON transactions (trx_id);
CREATE INDEX transactions_seq ON transactions (seq);


/* binary traces from state history. The writer may only choose to
   write traces for the last few million blocks */
CREATE TABLE traces
(
 block_num         BIGINT,
 seq               BIGINT,
 trace             BLOB,
 PRIMARY KEY (block_num, seq)
);


/* transaction receipts: every account involved in a transaction and relevant
   range of recv_sequence for each account */
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


/* as the account_receipts view has block_date in partitioning key,
   this table indexes the dates where an account has any transactions */
CREATE TABLE receipt_dates
(
 account_name           ASCII,
 block_date             BIGINT,
 PRIMARY KEY (account_name, block_date)
);



/* index of transactions for each contract and action */
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

/* dates where each (contract, action) tuple has a transaction */
CREATE TABLE action_dates
(
 contract               ASCII,
 action                 ASCII,
 block_date             BIGINT,
 PRIMARY KEY ((contract, action), block_date)
);




/* ABI for each contract, in binary form */
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

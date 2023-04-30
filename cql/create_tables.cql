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
 receiver               ASCII,
 contract               ASCII,
 action                 ASCII,
 recv_sequence          BIGINT,
 PRIMARY KEY (block_num, seq, receiver, recv_sequence, contract, action)
);


/* as the account_receipts view has block_date in partitioning key,
   this table indexes the dates where an account has any transactions */
CREATE TABLE receipt_dates
(
 receiver               ASCII,
 block_date             BIGINT,
 PRIMARY KEY (receiver, block_date)
);






/* ABI for each contract, in binary form */
CREATE TABLE abi_history
(
 block_num              BIGINT,
 contract               ASCII,
 abi_raw                BLOB,
 PRIMARY KEY (block_num, contract)
);

Chronos: a new history solution for Antelope blockchains
========================================================

This is a prototype that stores and indexes the transaction traces of
an Antelope blockchain in a ScyllaDB database.

[ScyllaDB](https://www.scylladb.com/) is an open-source
re-implementation of Apache Cassandra database. It's faster because
it's implemented in C++, andit supports data sharding.

The data model of ScyllaDB is that each table or materialized view is
a hash of sorted trees: the partition key is an unsortable hash key,
and each partition is a sorted tree. Also each tree cannot contain
more than 2 billion rows. This implies the limitations and
restrictions on the application data model.

The Chronos data model uses the block number as partition key in its
tables, so that complete blocks can be quickly deleted on microforks.

Also per-account and per-contract indexing tables are partitioned by
block_date, which is basically a millisecond timestamp of 00:00 UTC on
the date when the transaction has happened.

Chronos is implemented as a plugin for
[Chronicle](https://github.com/EOSChronicleProject/eos-chronicle).


## Performance benchmarks

### WAX mainnet test

Testing environment: a ScyllaDB cluster on four AX101 servers at
Hetzner in the same datacenter (16-core AMD Ryzen 9 5950X, 128GB ECC
RAM, 2 x 3.84 TB NVMe drives in RAID0, running XFS filesystem).

The keyspace is configured with `replication_factor=3`. The writer is
using an AMD Ryzen 7 3700X 8-Core Processor, sharing the same server
with WAX state history node.

The test run has processed the blocks 165855938-156978600 on WAX
mainnet in 17.5 hours, which gives an estimate of 500k blocks per
hour, or 12M blocks per day. The writer was sending about 16-17
thousand transactions per second to the database.

Storage use:

```
root@scylla-fi01 ~ # nodetool status
Datacenter: hel_dc5
===================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address        Load       Tokens       Owns    Host ID                               Rack
UN  65.109.115.25  767.42 GB  256          ?       1e4a0e34-45db-4b13-a559-2b06e04a9f40  rack1
UN  65.109.89.138  827.66 GB  256          ?       0ba9877a-08c4-4efe-a21f-90bf8492cb3b  rack1
UN  65.109.88.214  836.72 GB  256          ?       aab1ec1a-37be-409c-873a-93fcc4846198  rack1
UN  65.109.50.146  823.87 GB  256          ?       4dffa6b6-a6ce-48f0-b8b7-a9c81af745b4  rack1
```

As it's a 3x replication on 4 servers, the average amount of storage
for a single copy would be 1085GB for 8877338 blocks, or 122GB per
million blocks.

This data density is approximately the same as observed on a WAX
Hyperion server.


### Proton mainnet test

The same server environment was also used for Proton mainnet tests. As
the history size is smaller, it is easier to compare the performance
with Hyperion. The full Proton history as of this writing was indexed
in about 12 hours.

Storage use:

```
root@scylla-fi01 ~ # nodetool status
Datacenter: hel_dc5
===================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address        Load       Tokens       Owns    Host ID                               Rack
UN  65.109.115.25  188.42 GB  256          ?       1e4a0e34-45db-4b13-a559-2b06e04a9f40  rack1
UN  65.109.89.138  202.17 GB  256          ?       0ba9877a-08c4-4efe-a21f-90bf8492cb3b  rack1
UN  65.109.88.214  204.32 GB  256          ?       aab1ec1a-37be-409c-873a-93fcc4846198  rack1
UN  65.109.50.146  201.29 GB  256          ?       4dffa6b6-a6ce-48f0-b8b7-a9c81af745b4  rack1
```

This gives 265.40 GB for a single-copy installation. The full Proton Hyperion database is 793 GB.

Also, the full Proton
[Memento](https://github.com/Antelope-Memento/antelope_memento)
database on Postgres, using ZFS compression, occupies 563 GB.



# License and copyright

Source code repository: https://github.com/EOSChronicleProject/chronos

Copyright 2023 cc32d9@gmail.com

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.



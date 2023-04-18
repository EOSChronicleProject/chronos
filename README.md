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


### Proton mainnet test

Testing environment: a ScyllaDB cluster on four AX101 servers at
Hetzner in the same datacenter (16-core AMD Ryzen 9 5950X, 128GB ECC
RAM, 2 x 3.84 TB NVMe drives in RAID0, running XFS filesystem).

The keyspace is configured with `replication_factor: 3`. The writer is
using an AMD Ryzen 7 3700X 8-Core Processor, sharing the same server
with WAX state history node.

The full Proton history as of this writing was indexed in about 12
hours.

Storage use:

```
root@scylla-fi01 ~ # nodetool status
Datacenter: hel_dc5
===================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address        Load       Tokens       Owns    Host ID                               Rack
UN  65.109.115.25  305.73 GB  256          ?       1e4a0e34-45db-4b13-a559-2b06e04a9f40  rack1
UN  65.109.89.138  327.68 GB  256          ?       0ba9877a-08c4-4efe-a21f-90bf8492cb3b  rack1
UN  65.109.88.214  330.13 GB  256          ?       aab1ec1a-37be-409c-873a-93fcc4846198  rack1
UN  65.109.50.146  324.72 GB  256          ?       4dffa6b6-a6ce-48f0-b8b7-a9c81af745b4  rack1
```

This gives 429.42 GB for a single-copy installation. The full Proton Hyperion database is 793 GB.

Also, the full Proton
[Memento](https://github.com/Antelope-Memento/antelope_memento)
database on Postgres, using ZFS compression, occupies 563 GB.


### Full WAX mainet test

The same test environment as above, and the keyspace is configured
with `replication_factor: 1`.

The full WAX mainnet history as of block 240748585 was indexed in
about 4.5 days. The ingestion was mostly limited by the storage I/O
speed on the ScyllaDB servers, so theoretically it could be even
faster. The CPU on ScyllaDB servers was occupied at about 80% during
the scan.

The average indexing speed was 520 blocks per second in the range of
blocks 238731156-240618653, storing roughly 50k transactions per
second in 250k-270k database inserts per second.


Storage use:

```
root@scylla-fi01 ~ # nodetool status
Datacenter: hel_dc5
===================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address        Load       Tokens       Owns    Host ID                               Rack
UN  65.109.115.25  3.49 TB    256          ?       1e4a0e34-45db-4b13-a559-2b06e04a9f40  rack1
UN  65.109.89.138  3.5 TB     256          ?       0ba9877a-08c4-4efe-a21f-90bf8492cb3b  rack1
UN  65.109.88.214  3.43 TB    256          ?       aab1ec1a-37be-409c-873a-93fcc4846198  rack1
UN  65.109.50.146  3.56 TB    256          ?       4dffa6b6-a6ce-48f0-b8b7-a9c81af745b4  rack1
```

Thus, a single copy of the full history occupies 14TB of storage. A
full Hyperion server uses 22.5TB of storage as of today.





### WAX test with HDD storage

The test looks at the possibility to use Chronos with hard drives. The
test server at Hetzner is: 8-core Intel Xeon W-2245 CPU, 128GB RAM,
10x 16TB HDD SATA, 2x U.2 NVMe 960GB.

The NVME in RAID1 array are serving the OS and
`/var/lib/scylla/commitlog` partition.

The hard drives are grouped into 5 software mirrors, and a software
RAID0 on top of them. A 50TB partition is formatted with XFS and
mounted as `/var/lib/scylla`.

As mirror synchronization takes too much time, `sysctl
dev.raid.speed_limit_max=0` was used to stop the resynching.

The Chronos scanner demonstrated the performance of about 40 blocks
per second on such a server for WAX blocks in the range
197219016-197290173.

Overall, such a server would be alright for a database that does not
need too much of catching up. It would probably make sense to scan the
blockchain on a faster and more expensive server, and clone ScyllaDB
to such a cheaper server for further processing.








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




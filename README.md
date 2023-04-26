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


## Indexing performance benchmarks (with all traces)


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


### Full WAX mainnet test

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



### WAX test with 3x replication

The server hardware as above, but the keyspace is configured with
replication factor 3.

The average indexing speed for blocks 203282164-197982619 was 245
blocks per second, storing approximately 20-25k transactions per
second, using approxiomately 180k database inserts per second.

Also, running on a single server with replication factor 1 resulted in
approximately the same performance.


### WAX test with remote writer

Previous tests were made within the same datacenter (Hetzner
Helsinki). If the writer and the ScylaDB servers are in different
datacenters, the performance degrades a lot. In this test, the
ScyllaDB was on a server in Germany (25ms round-trip ping).

It is still possible to load the ScyllaDB Server CPU and storage I/O
with the work, but the writer needs to execute certain tasks
synchronously, and they slow down the whole process a lot. For
example, on receiving a fork event from the blockchain, the server
needs to delete a few blocks from the database. 25ms delay is slowing
down such tasks a lot.


### Server CPU performance

While processing the past histiory, the Chronos writer is able to
fully load the CPU on the ScyllaDB servers. In this regard, the number
of cores plays a significant role. For example, an 16-core AMD Ryzen 9
5950X allowed for about 40% faster processing than a 8-core Intel Xeon
W-2145.



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


### WAX test with low-cost hardware

In this test, we used 6 lowest-cost batemetal servers at Hetzner:
6-Core AMD Ryzen 5 3600, 64GB non-ECC RAM, 2x 480GB NVME drives. The
NVME drives were configured for RAID0 arrays and XFS filesystem.

One of the servers was rebooting every few hours, probably due to
faulty RAM. This allowed for testing the ScyllaDB resiliency.

With redundancy factor 3, the average writing speed was 169 blocks per
second in the range 228642204-226811517, sporting about 100k inserts
per second.

With redundancy factor 2, the average speed was 205 blocks per second.

With redundancy factor 1, the average speed was 364 blocks per second.

When one of the server crashed, it took about an hour until it
restored the operation.


## Indexing performance (without all traces)

On the low-cost hardware cluster described above, if traces are not
stored, the writer is about 25% faster.



## Querying benchmarks

The queries were made with the [JavaScript
client](https://github.com/EOSChronicleProject/chronos-client-npm) in
the same environment as the full WAX history test. The client was run
on the same server where the Chronos writer was running.

The JavaScript client that is available for ScyllaDB is designed for
Cassandra database, and it is not shard-aware. A shard-aware client
would potentially be faster. Also a lot of CPU processing time is
spent on the client side to decode the traces, sort them by sequence
number, and generate the output JSON.

1. Full history of `cc32dninexxx`: 155 transactions in 0.65s.

```
root@dev01:/opt/src/chronos-client-npm# time node lib/src/util/chronos_cli.js --host=scylla-fi01.dev.binfra.one --dc=hel_dc5 --username=chronos_ro --password=chronos_ro acc --account=cc32dninexxx --maxrows=10000 >/tmp/x

real    0m0.644s
user    0m0.945s
sys     0m0.050s

root@dev01:/opt/src/chronos-client-npm# cat /tmp/x | jq | fgrep '"block_num"' | wc
    155     310    4574
```

2. Last 1000 transactions of `atomicassets`: 3s

```
# the script prints many decoding errors, as it fails to decode some action arguments.

root@dev01:/opt/src/chronos-client-npm# time node lib/src/util/chronos_cli.js --host=scylla-fi01.dev.binfra.one --dc=hel_dc5 --username=chronos_ro --password=chronos_ro acc --account=atomicassets --maxrows=1000 >/tmp/x

real    0m2.925s
user    0m4.289s
sys     0m0.260s
```

3. Last 10000 transactions of `atomicassets`: 18.4s

```
root@dev01:/opt/src/chronos-client-npm# time node lib/src/util/chronos_cli.js --host=scylla-fi01.dev.binfra.one --dc=hel_dc5 --username=chronos_ro --password=chronos_ro acc --account=atomicassets --maxrows=10000 >/tmp/x

real    0m18.315s
user    0m24.493s
sys     0m1.289s

root@dev01:/opt/src/chronos-client-npm# cat /tmp/x | jq | fgrep '"block_num"' | wc
  10000   20000  300000
```

4. Last 10000 transactions for `m.federation`: 4.5s

```
root@dev01:/opt/src/chronos-client-npm# time node lib/src/util/chronos_cli.js --host=scylla-fi01.dev.binfra.one --dc=hel_dc5 --username=chronos_ro --password=chronos_ro acc --account=m.federation --maxrows=10000 >/tmp/x

real    0m4.547s
user    0m6.322s
sys     0m0.347s

root@dev01:/opt/src/chronos-client-npm# cat /tmp/x | jq | fgrep '"block_num"' | wc
  10000   20000  300000
```


5. Last 10000 transactions for `atomicmarket`: 27.7s

```
root@dev01:/opt/src/chronos-client-npm# time node lib/src/util/chronos_cli.js --host=scylla-fi01.dev.binfra.one --dc=hel_dc5 --username=chronos_ro --password=chronos_ro acc --account=atomicmarket --maxrows=10000 >/tmp/x

real    0m27.658s
user    0m38.654s
sys     0m1.921s

root@dev01:/opt/src/chronos-client-npm# cat /tmp/x | jq | fgrep '"block_num"' | wc
  10000   20000  300000
```


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




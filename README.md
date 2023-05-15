Chronos: a new history solution for Antelope blockchains
========================================================

This is a prototype that indexes the transaction traces of an Antelope
blockchain in a ScyllaDB database.

[ScyllaDB](https://www.scylladb.com/) is an open-source
re-implementation of Apache Cassandra database. It's faster because
it's implemented in C++, and it supports data sharding.

The database architecture allows for an enormous data ingestion
performance. A single server can handle about 100k inserts per second
with secondary indexes and materialized views, or even 500k inserts
per second without secondary indexes.

The data model of ScyllaDB is that each table or materialized view is
a hash of sorted trees: the partition key is an unsortable hash key,
and each partition is a sorted tree. Also each tree cannot contain
more than 2 billion rows. This implies the limitations and
restrictions on the application data model.

The Chronos data model uses the block number as partition key in its
tables, so that complete blocks can be quickly deleted on
microforks. Also, the per-account and per-contract materialized views
are partitioned by `block_date`, which is basically a millisecond
timestamp of 00:00 UTC on the date when the transaction has happened.

Chronos is implemented as a plugin for
[Chronicle](https://github.com/EOSChronicleProject/eos-chronicle).

Chronos would normally store only the indexing information, but not
the transaction traces. In the future, this information will be used
to fetch the blocks directly from Antelope state history archive. Also
Chronos can store copies of traces in ScyllaSDB for recent blocks for
a faster access (automatic deletion of older traces is not implemented
yet).





## Indexing performance benchmarks

WAX Mainnet was utilized as a reference dataset for testing because of
a large footprint and known benchmarks with Hyperion software by EOS
Rio.

At the moment, the global receipt counter on WAX is around 79.3
billion. A full Hyperion database occupies about 25TB on NVME storage.

Test environment:

* Antelope state history: 8-core AMD Ryzen 7 3700X, 2x NVME, 10x HDD,
  128GB RAM. In addition to the blockchain node, chronos was running
  on the same server.

* ScyllaDB: two servers with redundancy factor 1, 16-core AMD Ryzen 9
  5950X, 128GB RAM, one NVME for OS and commitlog, two NVMEs in RAID0
  for ScyllaDB data.

All 3 servers were at the same location at Hetzner Finland.

Full indexing up to the block number 245340556 took approximately 6
days. The indexer was processing about 300 blocks per second in the
most active part of the history (after 140M blocks). The total
ScyllaDB data size was 6.3TB, spit almost evenly across two servers.

The indexing performance was mainly limited by the CPU and storage
performance on ScyllaDB servers. Adding more servers or more NVME
drives could increase the indexing performance up to two-fold. The
NVME drives were almost 100% busy during the indexing, and the CPU was
about 10% idle.

The EOS Mainnet global sequence number is currently at 358.4
billion. Assuming hat the processing time and storage space are
approximately proportional to the number of action receipts, a full
EOS history index would occupy 28TB of storage and would take about a
month to complete (but 28TB would have to be split among a bigger
number of NVME drives, so the indexing performance would be
potentially higher).






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

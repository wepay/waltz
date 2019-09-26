---
id: on-disk-data-structures
title: On-Disk Data Structures
---

Waltz Storage provides persistency of data. It stores transaction data in its local disk.

## Directory Structure

Waltz Storage stores transaction data in the local file system. The root directory is called the storage directory which is configured using a configuration file. The storage directory contains the control file (waltz-storage.ctl) which contains a version information, creation timestamp and partition information. Under the storage directory, there are partition directories. Each partition directory contains data files and index files. For each partition, transaction data are split into segments chronologically. A new segment is created when the current segment grow beyond the configured size. Each segment consists of a data file and an index file.

```
<storage directory>/                # the root directory of the storage (configurable)
    waltz-storage.ctl               # the control file
    0/                              # the directory for partition 0
        0000000000000000000.seg     # the segment data file. The file name is
                                    # <first transaction id in the segment>.seq
        0000000000000000000.idx     # the segment's index file
        ....
    1/
        ....
 ```

## Control File

### Control File Header

The control file begins with the header which contains the following information.

| Field                    | Data Type | Size (bits)|
|--------------------------|-----------|------------|
| format version number    | int       |    32      |
| creation time            | long      |    64      |
| key                      | UUID      |   128      |
| the number of partitions | int       |    32      |
| reserved for future use  | -         |   768      |

The total header size is 128 bytes (1024 bits).

The key is UUID which is generated when the cluster is configured by CreateCluster utility. The key identifies the cluster to which the cluster it belongs. If an open request comes from a Waltz Server whose key does not match the key in the control file, Waltz Storage rejects the request.

### Control File Body

After the header follows the actual body of control data. It is a list of _Partition Info_. The number of _Partition Infos_ is the number of partitions recorded in the header.

|       Field                                  |    Data Type      | size(bits) |
|----------------------------------------------|-------------------|------------|
| partition id                                 |      int          |    32      |
| partition info struct 1 session id           |      long         |    64      |
| partition info struct 1 low-water mark       |      long         |    64      |
| partition info struct 1 local low-water mark |      long         |    64      |
| partition info struct 1 checksum             |      int          |    32      |
| partition info struct 2 session id           |      long         |    64      |
| partition info struct 2 low-water mark       |      long         |    64      |
| partition info struct 2 local low-water mark |      long         |    64      |
| partition info struct 2 checksum             |      int          |    32      |

Each _Partition Info_ record is 60 bytes (480 bits)

A partition info struct records the session ID, the low-water mark, the local low-water mark, and the checksum of the struct itself. The low-water mark is the high-water mark of the partition in the cluster when the session is successfully started. The local low-water mark is the highest valid transaction ID of the partition in the storage when the session is successfully started. The local low-water mark can be smaller than the low-water mark when the storage is falling behind.

Two partition info structs are updated alternately when a new storage session is started, and the update is immediately flushed to the disk. The checksum is checked when a partition of opened. Since the atomicity of I/O is not guaranteed, it is possible that an update is not completely written to the file when a fault occurs during I/O. If one of the structs has a checksum error, we ignore it and use the other struct, which means we rollback the partition. We assume at least one of them is always valid. If neither of structs is valid, we fail to open the partition.

## Segment Data File

### Data File Header

| Field | Data Type |
|-------|-----------|
| format version number | int |
| creation time | long |
| cluster key | UUID |
| partition id | int |
| first transaction ID | long |
| reserved for future use | 88 bytes |

The header size of 128 bytes. The cluster key is a UUID assigned to a cluster.

The first transaction ID is the ID of the first transaction in the segment.
The data file body is a list of transaction records. Each transaction record contains the following information.

### Transaction Record

| Field | Data Type |
|-------|-----------|
| transaction ID | long |
| request id | ReqId |
| transaction header | int |
| transaction data length | int |
| transaction data checksum | int |
| transaction data | byte[] |
| checksum | int |

When new records are written, Waltz Storage flushes the file channel to guarantee the record persistence before responding to Waltz Server. The index file is also updated, but flush is delayed to reduce physical I/Os until checkpoint. The checkpoint interval is 1000 transactions (hardcoded). When a checkpoint is reached, Waltz Storage flushes the index file before adding a new record. This means, if a fault occurs between checkpoints, we are not sure if the index is valid. So, the index file recovery is necessary every time Waltz Storage starts up. Waltz Storage scans the records from the last checkpoint and rebuild index for record after the last checkpoint.

## Segment Index File

### Index File Header

Exactly same as the data file header.

### Index File Body

Index File Body is an array of transaction record offsets.

| Field | Data Type |
|-------|-----------|
| transaction record offset | long |

Each element corresponds to a transaction in the segment. The array index is _&lt;transaction id&gt;_ - _&lt;first transaction id&gt;_. Each element is byte offsets of the transaction record in the data file.

## Checkpoint Interval

In the recovery process described above, the last known clean transaction ID is updated more often than a stable environment since it is updated during the recovery process. A drawback is that the number of transactions after the last known clean transaction ID can become large when no fault occurs for a long period of time. This is bad when a recovery requires a truncation to the last known clean transaction ID. So, Waltz provides a configuration parameter "storage.checkpointInterval" which is an interval in transactions for forced initiation of a new session.

## Handling Snapshot or Backup

Waltz does not provide a snapshot or backup making functionality. It is not a high priority at this moment since Waltz storage is fault tolerant. If necessary, use of a journaling file system like ZFS is a possible solution to this for now.

Let’s assume a snapshot is available somehow. We may restore stale storage files from a snapshot when storage files on a storage node is damaged by a disk failure or mistake. The issue is that the state information in Zookeeper and the state information storage becomes inconsistent. Waltz already handle this case. The storage is simply truncated to the last known clean transaction ID (recorded in the storage) to remove any possibly dirty transaction, then the catch-up process will be started and bring the storage up-to-date.


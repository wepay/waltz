---
id: waltz-storage
title: Waltz Storage
---

Waltz Storage is a storage server which provides persistency to Waltz. Its functionality is designed to be limited to relatively simple set of commands. Waltz centralizes the responsibility of transaction consistency and a fault recovery to Waltz Server. Waltz Storage has following ten request/response patterns.

1. Open Request → { Success Response | Failure Response }
2. Last Session Info Request → { Last Session Info Response | Failure Response }
3. Max Transaction ID Request → { Max Transaction ID Response | Failure Response }
4. Truncate Request → { Success Response | Failure Response }
5. Set Low-water Mark Request → { Success Response | Failure Response }
6. Append Request → { Success Response | Failure Response }
7. Record Header Request → { Record Header Response | Failure Response }
8. Record Request → { Record Response | Failure Response }
9. Record Header List Request → { Record Header List Response | Failure Response }
10. Record List Request → { Record List Response | Failure Response }

## Open Request

| Field | Data Type | Description |
|-------|-----------|-------------|
| Session ID | long | -1 |
| Sequence number | long | -1 |
| Partition ID | int  | -1 |
| Cluster key | UUID | A unique ID of the cluster |
| Number of partitions | int | The number of partitions |

Open request is the first request a Waltz server makes after a connection to a storage server is opened. A Waltz server sends a cluster key which is UUID assigned to a cluster when a cluster is created by an admin tool. The storage server compares it with the one in the control file. If they don’t match, it indicates there is a configuration error.

## Last Session Info Request

| Field | Data Type | Description |
|-------|-----------|-------------|
| Session ID | long | The store session ID |
| Sequence number | long | The message sequence number |
| Partition ID | int | The partition ID |

Last Session Info request is sent by the recovery manager when a replica connection is opened. It requests the information of the last store session of the storage partition on this storage server. It comes from Partition Info on the control file. The low-water mark is the high-water mark of the partition when the store session started. 

## Last Session Info Response

| Field | Data Type | Description |
|-------|-----------|-------------|
| Session ID | long | The store session ID |
| Sequence number | long | The message sequence number |
| Partition ID | int | The partition ID |
| Session ID | long | The last store session ID |
| Low-water Mark | long | The low-water mark of the last session |

## Max Transaction ID Request

| Field | Data Type | Description |
|-------|-----------|-------------|
| Session ID | long | The store session ID |
| Sequence number | long | The message sequence number |
| Partition ID | int | The partition ID |

This requests the max transaction ID of the specified storage partition. The transaction may not be  committed yet.

## Max Transaction ID Response

| Field | Data Type | Description |
|-------|-----------|-------------|
| Session ID | long | The store session ID |
| Sequence number | long | The message sequence number |
| Partition ID | int | The partition ID |
| Transaction ID | long | The max transaction ID of the partition on the storage server |

## Truncate Request

| Field | Data Type | Description |
|-------|-----------|-------------|
| Session ID | long | The store session ID |
| Sequence number | long | The message sequence number |
| Partition ID | int | The partition ID |
| Transaction ID | long | The max transaction ID to retain. Any transaction after this transaction ID will be removed. |

## Set Low-water Mark Request

| Field | Data Type | Description |
|-------|-----------|-------------|
| Session ID | long | The store session ID |
| Sequence number | long | The message sequence number |
| Partition ID | int | The partition ID |
| Low-water mark | long | The low-water mark which is the max transaction ID when this store session is started.  |

## Append Request

| Field | Data Type | Description |
|-------|-----------|-------------|
| Session ID | long | The store session ID |
| Sequence number | long | The message sequence number |
| Partition ID | int | The partition ID |
| Record List Length | int | The number of transaction records |
| Record List | Record[] | The list of records |


## RecordHeader
| Field | Data Type | Description |
|-------|-----------|-------------|
| Transaction ID | long | The transaction ID |
| Request ID | ReqId | Client generated unique request ID |
| Transaction Header | int | The transaction header |

## Record

| Field | Data Type | Description |
|-------|-----------|-------------|
| Transaction ID | long | The transaction ID |
| Request ID | ReqId | Client generated unique request ID |
| Transaction Header | int | The transaction header |
| Transaction Data Length | int | The length of transaction data |
| Transaction Data | byte[] | A byte array |
| Checksum | int | CRC32 of transaction data |

## Record Header Request

| Field | Data Type | Description |
|-------|-----------|-------------|
| Session ID | long | The store session ID |
| Sequence number | long | The message sequence number |
| Partition ID | int | The partition ID |
| Transaction ID | long | The transaction ID |

## Request Header Response

| Field | Data Type | Description |
|-------|-----------|-------------|
| Session ID | long | The store session ID |
| Sequence number | long | The message sequence number |
| Partition ID | int | The partition ID |
| Record header | RecordHeader | The transaction record header |

## Record Request

| Field | Data Type | Description |
|-------|-----------|-------------|
| Session ID | long | The store session ID
| Sequence number | long | The message sequence number
| Partition ID | int | The partition ID
| Transaction ID | long | The transaction ID

## Record Response

| Field | Data Type | Description |
|-------|-----------|-------------|
| Session ID | long | The store session ID |
| Sequence number | long | The message sequence number |
| Partition ID | int | The partition ID |
| Record | Record | The transaction record |

## Record Header List Request

| Field | Data Type | Description |
|-------|-----------|-------------|
| Session ID | long | The store session ID |
| Sequence number | long | The message sequence number |
| Partition ID | int | The partition ID |
| Transaction ID | long | The transaction ID |
| Max number of records | int | The maximum number of records to fetch |

## Request Header List Response

| Field | Data Type | Description |
|-------|-----------|-------------|
| Session ID | long | The store session ID |
| Sequence number | long | The message sequence number |
| Partition ID | int | The partition ID |
| Record header list Length | int | The number of record headers |
| Record header list | RecordHeader[] | The list of transaction record headers |

## Record List Request

| Field | Data Type | Description |
|-------|-----------|-------------|
| Session ID | long | The store session ID |
| Sequence number | long | The message sequence number |
| Partition ID | int | The partition ID |
| Transaction ID | long | The transaction ID |
| Max number of records | int | The maximum number of records to fetch |

## Record List Response

| Field | Data Type | Description |
|-------|-----------|-------------|
| Session ID | long | The store session ID |
| Sequence number | long | The message sequence number |
| Partition ID | int | The partition ID |
| Record List Length | int | The number of records |
| Record List | Record[] | The list of transaction record headers

## Success Response

| Field | Data Type | Description |
|-------|-----------|-------------|
| Session ID | long | The store session ID |
| Sequence number | long | The message sequence number |
| Partition ID | int  | The partition ID |

## Failure Response

| Field | Data Type | Description |
|-------|-----------|-------------|
| Session ID | long | The store session ID |
| Sequence number | long | The message sequence number |
| Partition ID | int | The partition ID |
| Exception | StorageRpcException | The exception information |

## StorageRpcException

| Field | Data Type | Description |
|-------|-----------|-------------|
| Message | String | The exception message |
| Stack Trace Length | int | The number of stack trace elements |
| Stack Trace Element *repeat | | |
| Class name | String  | The class name |
| Method name | String | The method name |
| File name | String | The file name |
| Line number | int  | The line number |


## Replica Assignments

The assignment of replicas to the storage serves are stored in Zookeeper. The ZNode path is `<cluster root>/store/assignment`. This data is initialized when a cluster is configured. It can be updated dynamically with Zookeeper CLI tool.

### Replica Assignments

| Field | Data Type | Description |
|-------|-----------|-------------|
| Map of Storage to Partition ID List *repeat | | |
| Connect String | String | The connect string (host:port) |
| Replica IDs | int[] | The array of Replica IDs |

## Group Descriptor

The descriptor of replica group assignments are stored in Zookeeper. The ZNode path is `<cluster root>/store/group`. This data is initialized when a cluster is configured. It can be updated dynamically with Zookeeper CLI tool.

### Group Descriptor

| Field | Data Type | Description |
|-------|-----------|-------------|
| Map of Connect String to Group ID *repeat | | |
| Connect String | String  | The connect string (host:port) |
| Group ID | Integer | The group ID it belongs to |


## Storage Administration

The Waltz storage server also exposes a separate port for administrative operations such as marking the storage node offline or online, or assigning a partition to the storage node. This client is implemented using the same client as the normal storage client, but with a different set of administrative messages as its protocol.

### Admin Open Request

| Field | Data Type | Description |
|-------|-----------|-------------|
| Sequence number | long | -1 |
| Cluster key | UUID | A unique ID of the cluster |
| Number of partitions | int | The number of partitions |

### Partition Assignment Request

| Field | Data Type | Description |
|-------|-----------|-------------|
| Sequence number | long | The message sequence number |
| Partition ID | int | The partition ID |
| Toggled | boolean | Create (true) or delete the partition (false) on the storage node |

### Partition Read Request

| Field | Data Type | Description |
|-------|-----------|-------------|
| Sequence number | long | The message sequence number |
| Partition ID | int | The partition ID |
| Toggled | boolean | Mark the partition as readable (true) or unreadable (false) on the storage node |

### Partition Write Request

| Field | Data Type | Description |
|-------|-----------|-------------|
| Sequence number | long | The message sequence number |
| Partition ID | int | The partition ID |
| Toggled | boolean | Mark the partition as writable (true) or unwritable (false) on the storage node |

### Admin Success Response

| Field | Data Type | Description |
|-------|-----------|-------------|
| Sequence number | long | The message sequence number |

### Admin Failure Response

| Field | Data Type | Description |
|-------|-----------|-------------|
| Sequence number | long | The message sequence number |
| Exception | StorageRpcException | The exception information |

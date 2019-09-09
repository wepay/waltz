---
id: server-storage-communication
title: Server-Storage Communication
---

## Quorum Writes

Waltz is a replicated transaction log. It does not use a master-slave replication method, but it uses a quorum write method. A quorum in Walt is equivalent to a majority vote. We will use "quorum" to mean "majority" in this document.

A quorum system has a number of benefits over master-slave replication. In master-slave replication, the master is the authoritative source of data, and slaves are always catching up with some latency. When a master dies due to a fault, we may want to promote one of slaves to a new master to continue a service. However, there is no guarantee that the slave has finished replication of all data before the death of the old master or knows the final commit decision that the master made. On the other hand, for a quorum system like Waltz a commit is consensus among participating storage servers, i.e., there is no central authority that may fail to propagate the commit information to other servers. A writer does not decide whether or not the write is committed, but it merely observe the commit is established by quorum. This distinction is important for recovery. A recovery process simply observes whether or not a particular write is committed by investigating the state of storages.

## Sessions

Waltz server is responsible for replicating transaction data to Waltz storages. Each Waltz server has a set of partitions assigned by the cluster manager. A partition is always assigned to a single server. No two servers write to the same partition at the same time. This is guaranteed by monotonically increasing session IDs. A server always establishes a session to access storage servers. When a server starts a new session, it acquires a new session ID (we use Zookeeper for this), and attaches the session ID to all messages to storage nodes. Storage nodes compare the session ID of the message with their current session ID. If the session ID of the message is greater than the ones they have, they take the session ID of the message as the most recent session ID and reject any message with lower session ids from then on. 

At the beginning of a session, Waltz server gathers storage states and figures out the last commit. Then it sends a truncate message to storages to remove any uncommitted transaction in storage nodes. If there is an unreachable storage server, the cleanup will be done later when the storage server becomes available again.

1. Get storage state information from Zookeeper
2. Gather storage state information from storage servers (last session ID, max transaction ID, the last known clean transaction ID)
3. See which storage node was active in the last session
4. If a storage server was not in the last session, simply truncate the log to the last known clean transaction ID.
5. Compute the highest commit transaction ID
6. Update storage information in zookeeper
7. Send the commit transaction ID to all available nodes (this becomes the new known clean transaction ID)
8. Clean up storage servers with dirty transactions.
 
Waltz prevents transaction logs from forking under any circumstance. Write requests are serialized by the server and streamed to storage servers. Ordering is guaranteed to TCP connection semantics and a single threaded processing per partition in a storage node. Furthermore, Waltz server creates a new session and runs the recovery whenever a storage node becomes unavailable even when there are enough number of healthy storage nodes. By design there is no way for a storage server to rejoin the session once it has left the session due to a fault.
 
Note that we don't assume a thing like a clean session close. A recovery is always run before a new session starts writing.

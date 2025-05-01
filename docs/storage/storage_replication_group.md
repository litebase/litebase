# Storage Replication Groups

In the cluster, storage nodes receive requests to read and write data from durable storage. When a cluster has more than one storage node, and a storage node receives a write request, the storage node must replicate the data to other storage nodes to ensure the data is durable.

When a storage node is ready to committ the write request to durable storage, it can perform the write, but it also must inform the other nodes in the replication group that the write was successful. If other nodes in the replciation group do not receive a successful commit of the write request before a deadline, another storage node can take over the write request and commit it to durable storage.

## Roles

There are two roles in the storage replication group:

* Writer
* Observer

### Writer

A writer can replicate data to other storage nodes in the replication group. Once acknowledged by a quorum of storage nodes, the write request is considered successful.

### Observer

In situations where the number of storage nodes is greater than 3 and not divisible by 3, an observer in the replication group is there to help achieve a quorum. An observer does not participate in writing to the replication group, but can take over committing write requests to durable storage if a writer fails.

## Forming a Replication Group

The process of forming a replication group is as follows:

1. Once a primary storage node has been elected, it will message the other 
storage nodes to check their replication group status.
2. The other storage nodes respond to the primary storage node with a message that indicates their replication group status.
3. If the primary storage node determines that the replication group is not formed, it will send a message to the other storage nodes to form the replication group.
4. The other storage nodes will respond to the primary storage node with a message that indicates the replication group has been formed.

If the cluster is already running, and storage node leadership changes, the new leader will check the replication group status of the other storage nodes. If the replication group is not formed, the new leader will send a message to the other storage nodes to form the replication group.

### Changing Replication Groups

While a storage node has pending write requests, it cannot change its replication group. To change the replication group, the storage node must first commit all pending write requests to durable storage.


from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from waltz_ducktape.tests.produce_consume_validate import ProduceConsumeValidateTest
from waltz_ducktape.tests.validation.node_bounce_scheduler import NodeBounceScheduler


class RecoveryTest(ProduceConsumeValidateTest):
    """
    Test Waltz recovery by performing partition management operations
    with CLI tools, including scaling up replica, moving storage node
    online/offline before/after maintenance, and so on.
    """
    def __init__(self, test_context):
        super(RecoveryTest, self).__init__(test_context=test_context, num_storage_nodes=4)

    @cluster(nodes_spec={'storage':4, 'server':2, 'client':1})
    @parametrize(partition=1, txn_per_client=100, num_clients=1, interval=60, timeout=240)
    def test_scale_up_replica_for_partition(self, partition, txn_per_client, num_clients, interval, timeout):
        # new_replica will not be added to zookeeper during setup
        # src_replica will be the source that new replica recovers from
        src_replica_idx = 0
        new_replica_idx = 3
        self.set_storage_nodes_to_skip([new_replica_idx])

        self.run_produce_consume_validate(lambda: self.scale_up_replica(src_replica_idx, new_replica_idx, partition,
                                                                        txn_per_client, num_clients, interval, timeout))

    @cluster(nodes_spec={'storage':4, 'server':2, 'client':1})
    @parametrize(partition=1, txn_per_client=500, num_clients=1, interval=60, timeout=240)
    def test_recovery_fail_when_replica_offline(self, partition, txn_per_client, num_clients, interval, timeout):
        self.run_produce_consume_validate(lambda: self.replica_back_online(partition, txn_per_client, num_clients, interval, timeout))

    def scale_up_replica(self, src_node_idx, new_node_idx, partition, txn_per_client, num_clients, interval, timeout):
        """
        A validate function to test scaling up replica for given partition.
        """
        # Step 1: Produce transactions with current cluster.
        # Trigger recovery to update replicas' partition low watermark.
        cmd = self.client_cli.validate_txn_cmd(partition, txn_per_client, num_clients, interval, -1)
        self.verifiable_client.start(cmd)
        self._trigger_recovery(bounce_replica_idx=1)
        wait_until(lambda: self.verifiable_client.task_complete() == True, timeout_sec=timeout,
                   err_msg="verifiable_client failed to complete task in %d seconds." % timeout)

        # Step 2: Add an empty replica and add partition to it.
        new_node = self.waltz_storage.nodes[new_node_idx]
        new_node_hostname = new_node.account.ssh_hostname
        port = self.waltz_storage.port
        admin_port = self.waltz_storage.admin_port
        storage = self.get_host(new_node_hostname, admin_port)
        self.storage_add_partition(storage=storage, partition=partition)

        # Step 3: Run recovery operation on new replica.
        # Source replica's partition low watermark will be used as target for recovery.
        src_node = self.waltz_storage.nodes[src_node_idx]
        src_node_hostname = src_node.account.ssh_hostname
        src_storage = self.get_host(src_node_hostname, admin_port)
        dst_storage = self.get_host(new_node_hostname, port)
        self.storage_recover_partition(source_storage=src_storage, destination_storage=dst_storage,
                               partition=partition, batch_size=20)

        # Step 4: Check if new replica catch up with source replica.
        src_node_local_low_watermark = self.get_storage_local_low_watermark(self.get_host(src_node_hostname, admin_port), partition)
        new_node_local_low_watermark = self.get_storage_local_low_watermark(self.get_host(new_node_hostname, admin_port), partition)
        assert -1 < src_node_local_low_watermark == new_node_local_low_watermark,\
            "Partition recovery failed on storage %s, with local low watermark = %d"\
            % (new_node_hostname, new_node_local_low_watermark)

        # Step 5: Mark new replica online for reads and writes
        self.storage_set_availability(storage=storage, partition=partition, availability="online")

        # Step 6: Add new replica to the replica set in ZooKeeper
        storage=self.get_host(new_node_hostname, port)
        self.zk_add_storage_node(storage=storage, storage_admin_port=admin_port, group=new_node_idx)
        self.zk_assign_partition(storage=storage, partition=partition)

        # Step 7: Produce transactions after adding new replica.
        # Trigger recovery to update new replica's partition low watermark (for assertion)
        cmd = self.client_cli.validate_txn_cmd(partition, txn_per_client, num_clients, interval, 99)
        self.verifiable_client.start(cmd)
        self._trigger_recovery(bounce_replica_idx=1)
        wait_until(lambda: self.verifiable_client.task_complete() == True, timeout_sec=timeout,
                   err_msg="verifiable_client failed to complete task in %d seconds." % timeout)

        # Step 8: Check if new transactions can reach new replica.
        pre_new_node_local_low_watermark = new_node_local_low_watermark
        cur_new_node_local_low_watermark = self.get_storage_local_low_watermark(self.get_host(new_node_hostname, admin_port), partition)

        assert cur_new_node_local_low_watermark > pre_new_node_local_low_watermark, \
            "No new transactions reach new replica, expect local low water mark greater than {}, but got {}"\
            .format(pre_new_node_local_low_watermark, cur_new_node_local_low_watermark)

        return self.verifiable_client.get_validation_result()

    def replica_back_online(self, partition, txn_per_client, num_clients, interval, timeout):
        """
        A validate function to test recovery when replica back online
        """
        # Step 1: Produce a number of transactions.
        cmd = self.client_cli.validate_txn_cmd(partition, txn_per_client, num_clients, interval, -1)
        self.verifiable_client.start(cmd)

        # Step 2: Set storage node 0 offline, quorum still meet.
        node0 = self.waltz_storage.nodes[0]
        hostname = node0.account.ssh_hostname
        admin_port = self.waltz_storage.admin_port
        storage = self.get_host(hostname, admin_port)
        self.storage_set_availability(storage=storage, partition=partition, availability="offline")

        # Step 3: Wait until all transactions appended.
        wait_until(lambda: self.verifiable_client.task_complete() == True, timeout_sec=timeout,
                   err_msg="verifiable_client failed to complete task in %d seconds." % timeout)

        # Step 4: Set storage node 0 online.
        storage_session_id_offline = self.get_storage_session_id(self.get_host(hostname, admin_port), partition)
        self.storage_set_availability(storage=storage, partition=partition, availability="online")

        # Step 5: Wait until recovery complete on current replica.
        wait_until(lambda: self.get_storage_session_id(self.get_host(hostname, admin_port), partition) == storage_session_id_offline + 1,
                   timeout_sec=timeout, err_msg="recovery failed to complete on current replica.")

    def _trigger_recovery(self, bounce_replica_idx):
        """
        Bounce a storage node to trigger recovery procedure. This will force
        current replicas to update their low watermark for all partitions.
        """
        cmd_list = [
            {"action": NodeBounceScheduler.IDLE},
            {"action": NodeBounceScheduler.KILL_PROCESS, "node": bounce_replica_idx}
        ]
        storage_node_bounce_scheduler = NodeBounceScheduler(service=self.waltz_storage, interval=3,
                                                            stop_condition=lambda: self.verifiable_client.task_complete(),
                                                            iterable_cmd_list=iter(cmd_list))
        storage_node_bounce_scheduler.start()

from random import randrange
from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.utils.util import wait_until
from waltz_ducktape.tests.produce_consume_validate import ProduceConsumeValidateTest


class ScalabilityTest(ProduceConsumeValidateTest):
    """
    Test Waltz scalability by scaling up and scaling down replicas.
    """
    MIN_CLUSTER_SPEC = ClusterSpec.from_list([
        {'cpu':1, 'mem':'1GB', 'disk':'25GB', 'additional_disks':{'/dev/sdb':'100GB'}, 'num_nodes':4},
        {'cpu':1, 'mem':'3GB', 'disk':'15GB', 'num_nodes':2},
        {'cpu':1, 'mem':'1GB', 'disk':'25GB', 'num_nodes':1}])

    def __init__(self, test_context):
        super(ScalabilityTest, self).__init__(test_context=test_context, num_storage_nodes=4)

    @cluster(cluster_spec=MIN_CLUSTER_SPEC)
    @parametrize(num_active_partitions=1, txn_per_client=500, num_clients=1, interval=100, timeout=240)
    def test_scale_up_replica(self, num_active_partitions, txn_per_client, num_clients, interval, timeout):
        src_node_idx = 0
        added_node_idx = 3
        # new replica will not receive appends until offline recovery completes
        self.set_storage_nodes_to_ignore([added_node_idx])
        self.run_produce_consume_validate(lambda: self.scale_up_replica(src_node_idx, added_node_idx, num_active_partitions,
                                                                        txn_per_client, num_clients, interval, timeout))

    def scale_up_replica(self, src_node_idx, added_node_idx, num_active_partitions, txn_per_client, num_clients, interval, timeout):
        """
        A validate function to test scaling up replica for given partition.

        :param src_node_idx: The index of source node, where new replica recovers from
        :param added_node_idx: The index of replica to add
        :param num_active_partitions: Number of active partitions
        :param txn_per_client: Number of transactions per client
        :param num_clients: Number of total clients
        :param interval: Average interval(millisecond) between transactions
        :param timeout: Test timeout
        :returns: Validation result
        """
        port = self.waltz_storage.port
        admin_port = self.waltz_storage.admin_port
        src_node = self.waltz_storage.nodes[src_node_idx]
        src_node_hostname = src_node.account.ssh_hostname
        src_storage = self.get_host(src_node_hostname, admin_port)
        added_node = self.waltz_storage.nodes[added_node_idx]
        added_node_hostname = added_node.account.ssh_hostname
        added_storage = self.get_host(added_node_hostname, admin_port)
        partition = randrange(num_active_partitions)

        # Step 1: Produce transactions with current cluster.
        cmd = self.client_cli.validate_txn_cmd(num_active_partitions, txn_per_client, num_clients, interval)
        self.verifiable_client.start(cmd)
        wait_until(lambda: self.is_max_transaction_id_updated(src_storage, port, partition, -1), timeout_sec=timeout)

        # Step 2: Trigger recovery to update source replicas' low watermark.
        self.trigger_recovery(bounce_node_idx=src_node_idx)
        wait_until(lambda: self.is_triggered_recovery_completed(), timeout_sec=timeout)
        src_node_local_low_watermark = self.get_storage_local_low_watermark(self.get_host(src_node_hostname, admin_port), partition)

        # Step 3: Add an empty replica and add partition to it.
        self.storage_add_partition(storage=added_storage, partition=partition)

        # Step 4: Mark added replica offline for reads and writes
        self.storage_set_availability(storage=added_storage, partition=partition, online=False)

        # Step 5: Run recovery operation on new replica.
        # Source replica's partition low watermark will be used as target for recovery.
        self.storage_recover_partition(source_storage=src_storage, destination_storage=added_storage,
                                       destination_storage_port=port, partition=partition, batch_size=20)

        # Step 6: Check if new replica catch up with source replica.
        added_node_max_transaction_id = self.get_storage_max_transaction_id(self.get_host(added_node_hostname, admin_port), port, partition, True)
        assert src_node_local_low_watermark == added_node_max_transaction_id, \
            "Partition recovery failed on storage {}, expected max transaction ID = {}, actual max transaction ID = {}" \
            .format(added_node_hostname, src_node_local_low_watermark, added_node_max_transaction_id)

        # Step 7: Mark new replica online for reads and writes
        self.storage_set_availability(storage=added_storage, partition=partition, online=True)

        # Step 8: Add new replica to the replica set in ZooKeeper
        storage=self.get_host(added_node_hostname, port)
        self.zk_add_storage_node(storage=storage, storage_admin_port=admin_port, group=added_node_idx)
        self.zk_assign_partition(storage=storage, partition=partition)

        # Step 9: Produce transactions after adding new replica.
        wait_until(lambda: self.verifiable_client.task_complete() == True, timeout_sec=timeout,
                   err_msg="verifiable_client failed to complete task in %d seconds." % timeout)

        # Step 10: Check if new transactions can reach new replica.
        expected_max_transaction_id = -1 + num_clients * txn_per_client
        added_node_max_transaction_id = self.get_storage_max_transaction_id(self.get_host(added_node_hostname, admin_port), port, partition)
        assert added_node_max_transaction_id == expected_max_transaction_id, \
            "New transactions failed to reach new replica, expected max transaction ID = {}, actual max transaction ID = {}" \
            .format(expected_max_transaction_id, added_node_max_transaction_id)

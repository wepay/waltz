from random import randrange
from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from waltz_ducktape.tests.produce_consume_validate import ProduceConsumeValidateTest


class RecoveryTest(ProduceConsumeValidateTest):
    """
    Test Waltz recovery by running offline recovery with CLI tools,
    including recover dirty replicas, bring up offline replica, and
    so on.
    """
    def __init__(self, test_context):
        super(RecoveryTest, self).__init__(test_context=test_context)

    @cluster(nodes_spec={'storage':3, 'server':2, 'client':1})
    @parametrize(num_active_partitions=1, txn_per_client=250, num_clients=1, interval=100, timeout=240)
    def test_recover_dirty_replica(self, num_active_partitions, txn_per_client, num_clients, interval, timeout):
        src_replica_idx = 0
        dst_replica_idx = 2
        self.run_produce_consume_validate(lambda: self.recover_dirty_replica(src_replica_idx, dst_replica_idx, num_active_partitions,
                                                                             txn_per_client, num_clients, interval, timeout))

    @cluster(nodes_spec={'storage':3, 'server':2, 'client':1})
    @parametrize(num_active_partitions=1, txn_per_client=250, num_clients=1, interval=100, timeout=240)
    def test_bring_replica_back_online(self, num_active_partitions, txn_per_client, num_clients, interval, timeout):
        offline_replica_idx = 0

        self.run_produce_consume_validate(lambda: self.bring_replica_back_online(offline_replica_idx, num_active_partitions, txn_per_client,
                                                                           num_clients, interval, timeout))

    def recover_dirty_replica(self, src_replica_idx, dst_replica_idx, num_active_partitions, txn_per_client, num_clients, interval, timeout):
        """
        A validate function to test offline recovery if a dirty replica.

        :param src_replica_idx: The index of source replica, where new replica recovers from
        :param dst_replica_idx: The index of destination replica
        :param num_active_partitions: Number of active partitions
        :param txn_per_client: Number of transactions per client
        :param num_clients: Number of total clients
        :param interval: Average interval(millisecond) between transactions
        :param timeout: Test timeout
        """
        port = self.waltz_storage.port
        admin_port = self.waltz_storage.admin_port
        src_node = self.waltz_storage.nodes[src_replica_idx]
        src_node_hostname = src_node.account.ssh_hostname
        src_storage = self.get_host(src_node_hostname, admin_port)
        dst_node = self.waltz_storage.nodes[dst_replica_idx]
        dst_node_hostname = dst_node.account.ssh_hostname
        dst_storage = self.get_host(dst_node_hostname, admin_port)
        partition = randrange(num_active_partitions)

        # Step 1: Submit transactions to all replicas.
        cmd = self.client_cli.validate_txn_cmd(num_active_partitions, txn_per_client, num_clients, interval)
        self.verifiable_client.start(cmd)
        wait_until(lambda: self.is_max_transaction_id_updated(src_storage, port, partition, -1), timeout_sec=timeout)

        # Step 2: Mark destination replica offline for reads and writes
        self.storage_set_availability(storage=dst_storage, partition=partition, online=False)

        # Step 3: Trigger recovery to update source replicas' low watermark.
        self.trigger_recovery(bounce_node_idx=src_replica_idx)
        wait_until(lambda: self.is_triggered_recovery_completed(), timeout_sec=timeout)
        src_node_local_low_watermark = self.get_storage_local_low_watermark(self.get_host(src_node_hostname, admin_port), partition)

        # Step 4: Run recovery operation on offline replica.
        # Source replica's partition low watermark will be used as target for recovery.
        self.storage_recover_partition(source_storage=src_storage, destination_storage=dst_storage,
                                       destination_storage_port=port, partition=partition, batch_size=20)

        # Step 5: Check if destination replica catch up with source replica.
        dst_node_max_transaction_id = self.get_storage_max_transaction_id(self.get_host(dst_node_hostname, admin_port), port, partition, True)
        assert src_node_local_low_watermark == dst_node_max_transaction_id, \
            "partition recovery failed on storage {}, expected max transaction ID = {}, actual max transaction ID = {}" \
            .format(dst_node_hostname, src_node_local_low_watermark, dst_node_max_transaction_id)

        # Step 6: Wait until validation complete.
        wait_until(lambda: self.verifiable_client.task_complete() == True, timeout_sec=timeout,
                   err_msg="verifiable_client failed to complete task in %d seconds." % timeout)

    def bring_replica_back_online(self, offline_replica_idx, num_active_partitions, txn_per_client, num_clients, interval, timeout):
        """
        A validate function to test if a replica can successfully recover when brought back online.

        :param offline_replica_idx: The index of offline replica
        :param num_active_partitions: Number of active partitions
        :param txn_per_client: Number of transactions per client
        :param num_clients: Number of total clients
        :param interval: Average interval(millisecond) between transactions
        :param timeout: Test timeout
        """
        admin_port = self.waltz_storage.admin_port
        node = self.waltz_storage.nodes[offline_replica_idx]
        hostname = node.account.ssh_hostname
        partition = randrange(num_active_partitions)

        # Step 1: Produce a number of transactions.
        cmd = self.client_cli.validate_txn_cmd(num_active_partitions, txn_per_client, num_clients, interval, -1)
        self.verifiable_client.start(cmd)

        # Step 2: Mark storage node 0 offline for reads and writes.
        storage = self.get_host(hostname, admin_port)
        self.storage_set_availability(storage=storage, partition=partition, online=False)
        storage_session_id_offline = self.get_storage_session_id(self.get_host(hostname, admin_port), partition)

        # Step 3: Mark storage node online. Wait until recovery is completed.
        self.storage_set_availability(storage=storage, partition=partition, online=True)
        wait_until(lambda: self.is_triggered_recovery_completed(), timeout_sec=timeout)

        # Step 4: Check if storage node's session ID bumps up by 1.
        storage_session_id_online = self.get_storage_session_id(storage, partition)
        assert storage_session_id_online == storage_session_id_offline + 1, \
               "recovery failed to complete on storage {}, expected session ID = {}, actual session ID = {}" \
               .format(hostname, storage_session_id_offline + 1, storage_session_id_online)

        # Step 5: Wait until all transactions appended.
        wait_until(lambda: self.verifiable_client.task_complete() == True, timeout_sec=timeout,
                   err_msg="verifiable_client failed to complete task in %d seconds." % timeout)

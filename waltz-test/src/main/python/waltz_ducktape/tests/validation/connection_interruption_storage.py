from waltz_ducktape.tests.produce_consume_validate import ProduceConsumeValidateTest
from ducktape.mark.resource import cluster
from ducktape.mark import parametrize
from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.utils.util import wait_until
from threading import Thread
from time import sleep
from random import randrange
import random


class ConnectionInterruptionServer(ProduceConsumeValidateTest):
    """
        Class for tests simulating network issues between server and storage.
    """

    MIN_CLUSTER_SPEC = ClusterSpec.from_list([
        {'cpu': 1, 'mem': '1GB', 'disk': '25GB', 'additional_disks': {'/dev/sdb': '100GB'}, 'num_nodes': 3},
        {'cpu': 1, 'mem': '3GB', 'disk': '15GB', 'num_nodes': 2},
        {'cpu': 1, 'mem': '1GB', 'disk': '25GB', 'num_nodes': 1}])

    def __init__(self, test_context):
        super(ConnectionInterruptionServer, self).__init__(test_context=test_context)

    @cluster(cluster_spec=MIN_CLUSTER_SPEC)
    @parametrize(num_active_partitions=1, txn_per_client=200, num_clients=1, interval=100, timeout=240, interruption_length=5, num_of_nodes_to_bounce=1)
    @parametrize(num_active_partitions=4, txn_per_client=200, num_clients=2, interval=100, timeout=300, interruption_length=5, num_of_nodes_to_bounce=1)
    @parametrize(num_active_partitions=1, txn_per_client=200, num_clients=1, interval=100, timeout=300, interruption_length=5, num_of_nodes_to_bounce=2)
    @parametrize(num_active_partitions=4, txn_per_client=200, num_clients=2, interval=100, timeout=300, interruption_length=5, num_of_nodes_to_bounce=2)
    def test_disconnect_storage_shortly(self, num_active_partitions, txn_per_client, num_clients, interval, timeout, interruption_length, num_of_nodes_to_bounce):
        self.run_produce_consume_validate(lambda: self.disconnect_reconnect_storage_node(num_active_partitions, txn_per_client, num_clients, interval, timeout, interruption_length, num_of_nodes_to_bounce))

    class StorageNodeInfo:
        def __init__(self, node):
            self.node = node
            self.node_hostname = node.account.ssh_hostname
            self.sum_number_of_transactions_before = 0

    def disconnect_reconnect_storage_node(self, num_active_partitions, txn_per_client, num_clients, interval, timeout, interruption_length, num_of_nodes_to_bounce):
        """
        A validate function to test bouncing network connection between server and storage. Verification of correctness
        is done by comparing expected number of stored transactions with the reality.

        :param num_active_partitions: Number of active partitions
        :param txn_per_client: Number of transactions per client
        :param num_clients: Number of waltz clients
        :param interval: Average interval(millisecond) between transactions
        :param timeout: Test timeout
        :param interruption_length: Length(milliseconds) of communication interruption between server and storage
        :param num_of_nodes_to_bounce: Number of storage nodes to bounce. This may affect the quorum
        """

        nodes_summary = []
        for node_number in random.sample(range(len(self.waltz_storage.nodes)), num_of_nodes_to_bounce):
            nodes_summary.append(self.StorageNodeInfo(self.waltz_storage.nodes[node_number]))

        admin_port = self.waltz_storage.admin_port
        port = self.waltz_storage.port

        # Step 1: Get sum of current max_transaction_ids
        for node_summary in nodes_summary:
            for partition in range(num_active_partitions):
                node_summary.sum_number_of_transactions_before += max(-1, self.get_storage_max_transaction_id(self.get_host(node_summary.node_hostname, admin_port), port, partition))

        # Step 2: Submit transactions to all replicas.
        validation_cmd = self.client_cli.validate_txn_cmd(num_active_partitions, txn_per_client, num_clients, interval)
        self.verifiable_client.start(validation_cmd)
        wait_until(lambda: self.is_max_transaction_id_updated(self.get_host(nodes_summary[0].node_hostname, admin_port), port, randrange(num_active_partitions), -1), timeout_sec=timeout)

        # Step 3: Disconnect storage
        for node_summary in nodes_summary:
            node_summary.node.account.ssh_capture("sudo iptables -I INPUT -p tcp --destination-port {} -j DROP".format(port))
            sleep(interruption_length)

        # Step 4: Reconnect storage
        for node_summary in nodes_summary:
            node_summary.node.account.ssh_capture("sudo iptables -D INPUT -p tcp --destination-port {} -j DROP".format(port))

        # Step 5: Wait until validation complete.
        wait_until(lambda: self.verifiable_client.task_complete() == True, timeout_sec=timeout,
                   err_msg="verifiable_client failed to complete task in %d seconds." % timeout)

        # Step 6: Verify total number of transactions matches number of transactions stored in waltz storage nodes
        for node_summary in nodes_summary:
            expected_number_of_transactions = node_summary.sum_number_of_transactions_before + txn_per_client * num_clients
            wait_until(lambda: expected_number_of_transactions == self.sum_of_transactions(node_summary, admin_port, port, num_active_partitions),
                       timeout_sec=timeout, err_msg="number of transactions stored in storage partition does not match with all the transactions sent by client. Client {}, Storage = {} after {} seconds" \
                .format(expected_number_of_transactions, self.sum_of_transactions(node_summary, admin_port, port, num_active_partitions), timeout))

    def sum_of_transactions(self, node_summary, admin_port, port, num_active_partitions):
        """
        :returns Sum of all stored transactions under active partitions in a storage node
        """

        sum = 0
        for partition in range(num_active_partitions):
            sum += self.get_storage_max_transaction_id(self.get_host(node_summary.node_hostname, admin_port), port, partition)
        return sum

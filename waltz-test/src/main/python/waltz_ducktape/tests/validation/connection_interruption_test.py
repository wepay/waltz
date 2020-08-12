from waltz_ducktape.tests.produce_consume_validate import ProduceConsumeValidateTest
from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.mark.resource import cluster
from ducktape.mark import parametrize
from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.utils.util import wait_until
from time import sleep
from random import randrange
from random import sample


class ConnectionInterruptionTest(ProduceConsumeValidateTest):
    """
        Class for tests simulating network issues corresponding to Waltz Server and Waltz Storage nodes
    """

    MIN_CLUSTER_SPEC = ClusterSpec.from_list([
        {'cpu': 1, 'mem': '1GB', 'disk': '25GB', 'additional_disks': {'/dev/sdb': '100GB'}, 'num_nodes': 3},
        {'cpu': 1, 'mem': '3GB', 'disk': '15GB', 'num_nodes': 2},
        {'cpu': 1, 'mem': '1GB', 'disk': '25GB', 'num_nodes': 1}])

    def __init__(self, test_context):
        super(ConnectionInterruptionTest, self).__init__(test_context=test_context)

    @cluster(cluster_spec=MIN_CLUSTER_SPEC)
    @parametrize(num_active_partitions=1, txn_per_client=150, num_clients=2, interval=600, timeout=360,
                 interrupt_duration=10, num_interruptions=3, delay_between_interruptions=25)
    @parametrize(num_active_partitions=4, txn_per_client=100, num_clients=2, interval=1000, timeout=300,
                 interrupt_duration=20, num_interruptions=1, delay_between_interruptions=20)
    def test_client_server_network_interruption(self, num_active_partitions, txn_per_client, num_clients, interval, timeout,
                                                interrupt_duration, num_interruptions, delay_between_interruptions):
        validation_cmd = self.client_cli.validate_txn_cmd(self.log_file_path, num_active_partitions, txn_per_client,
                                                          num_clients, interval)
        self.run_produce_consume_validate(lambda: self.client_server_network_interruption(validation_cmd, timeout,
                interrupt_duration, num_interruptions, delay_between_interruptions, num_active_partitions, interval/1000))

    @cluster(cluster_spec=MIN_CLUSTER_SPEC)
    @parametrize(num_active_partitions=1, txn_per_client=200, num_clients=1, interval=100, timeout=240,
                 interrupt_duration=5, num_of_nodes_to_bounce=2)
    @parametrize(num_active_partitions=4, txn_per_client=200, num_clients=2, interval=100, timeout=240,
                 interrupt_duration=5, num_of_nodes_to_bounce=2)
    @parametrize(num_active_partitions=1, txn_per_client=200, num_clients=1, interval=100, timeout=240,
                 interrupt_duration=5, num_of_nodes_to_bounce=1)
    @parametrize(num_active_partitions=4, txn_per_client=200, num_clients=2, interval=100, timeout=300,
                 interrupt_duration=5, num_of_nodes_to_bounce=1)
    def test_storage_node_network_interruption(self, num_active_partitions, txn_per_client, num_clients, interval, timeout,
                                               interrupt_duration, num_of_nodes_to_bounce):
        validation_cmd = self.client_cli.validate_txn_cmd(self.log_file_path, num_active_partitions, txn_per_client, num_clients, interval)
        self.run_produce_consume_validate(lambda: self.storage_node_network_interruption(validation_cmd, num_active_partitions,
                                          txn_per_client, num_clients, timeout, interrupt_duration, num_of_nodes_to_bounce))

    def drop_traffic_to_port(self, node, port):
        node.account.ssh_capture("sudo iptables -I INPUT -p tcp --destination-port {} -j DROP".format(port))

    def enable_traffic_to_port(self, node, port):
        node.account.ssh_capture("sudo iptables -D INPUT -p tcp --destination-port {} -j DROP".format(port))

    def client_server_network_interruption(self, validation_cmd, timeout, interrupt_duration, num_interruptions,
                                           delay_between_interruptions, num_active_partitions, processing_duration):
        """
        Set up waltz and interrupt network between a waltz client node and a server node.

        :param validation_cmd: The command that is send to ClientCli
        :param timeout: Test timeout
        :param interrupt_duration: Interval in seconds during which client won't be able to connect to server
        :param num_interruptions: Number of connection interruption cycles
        :param delay_between_interruptions: Interval in seconds that represents duration between network interruptions
        :param num_active_partitions: Number of active partitions
        :param processing_duration: Time in seconds within which it is safe to assume that processed transactions gets stored
        """

        partition = randrange(num_active_partitions)
        node_idx = self.get_server_node_idx(partition)

        # Start waltz cluster and wait until a storage node registers first transaction
        self.verifiable_client.start(validation_cmd)
        admin_port = self.waltz_storage.admin_port
        port = self.waltz_storage.port
        storage = self.get_host(self.waltz_storage.nodes[0].account.ssh_hostname, admin_port)
        wait_until(lambda: self.is_max_transaction_id_updated(storage, port, partition, -1), timeout_sec=timeout)

        node = self.waltz_server.nodes[node_idx]
        for interruption in range(num_interruptions):
            sleep(delay_between_interruptions)
            try:
                # disable connection on port
                self.drop_traffic_to_port(node, self.waltz_server.port)

                # verify that during network interruption number of stored transactions didn't increase
                # sleep time to assure that transactions from server are propagated to storage nodes
                sleep(processing_duration)
                cur_high_watermark = self.get_storage_max_transaction_id(storage, port, partition)
                sleep(max(interrupt_duration - processing_duration, 0))
                assert not self.is_max_transaction_id_updated(storage, port, partition, cur_high_watermark), \
                    'Network interruption failed, newly stored transactions detected'

            finally:
                """
                delete the added iptable rule as it is not removed with the end of waltz process and could persist on 
                waltz-server VM, if process is signaled to end (^C) or an exception is thrown during execution of the try block.
                """
                self.enable_traffic_to_port(node, self.waltz_server.port)

        wait_until(lambda: self.verifiable_client.task_complete() == True, timeout_sec=timeout,
                   err_msg="verifiable_client failed to complete task in %d seconds." % timeout)

    class StorageNodeInfo:
        """
        Representation of a storage node and number of transactions stored on a storage node
        """
        def __init__(self, node):
            self.node = node
            self.total_num_of_transactions = 0

    def storage_node_network_interruption(self, validation_cmd, num_active_partitions, txn_per_client, num_clients, timeout,
                                          interrupt_duration, num_of_nodes_to_bounce):
        """
        A validate function to test bouncing network connection between server and storage. Verification of correctness
        is done by comparing expected number of stored transactions with current transactions in the Waltz Cluster.

        :param validation_cmd: The command that is send to ClientCli
        :param num_active_partitions: Number of active partitions
        :param txn_per_client: Number of transactions per client
        :param num_clients: Number of waltz clients
        :param timeout: Test timeout
        :param interrupt_duration: Duration (milliseconds) of communication interruption between server and storage
        :param num_of_nodes_to_bounce: Number of storage nodes to bounce. This may affect the quorum
        """

        bounced_nodes = []
        for node_number in sample(range(len(self.waltz_storage.nodes)), num_of_nodes_to_bounce):
            bounced_nodes.append(self.StorageNodeInfo(self.waltz_storage.nodes[node_number]))

        admin_port = self.waltz_storage.admin_port
        port = self.waltz_storage.port

        # Step 1: Get current sum of transactions across partitions
        for bounced_node_info in bounced_nodes:
            for partition in range(num_active_partitions):
                bounced_node_info.total_num_of_transactions += max(0, self.get_storage_max_transaction_id(
                    self.get_host(bounced_node_info.node.account.ssh_hostname, admin_port), port, partition) + 1)

        # Step 2: Submit transactions to all replicas.
        self.verifiable_client.start(validation_cmd)
        wait_until(lambda: self.is_max_transaction_id_updated(self.get_host(bounced_nodes[0].node.account.ssh_hostname, admin_port), port,
                                                       randrange(num_active_partitions), -1), timeout_sec=timeout)
        try:
            # Step 3: Interrupt connection
            for bounced_node_info in bounced_nodes:
                self.drop_traffic_to_port(bounced_node_info.node, port)

            # Step 4: Verify that storage port is closed
            for bounced_node_info in bounced_nodes:
                partition = randrange(num_active_partitions)

                # RemoteCommandError raised when get_storage_max_transaction_id request fails
                # because connection to the storage node port is blocked
                try:
                    self.get_storage_max_transaction_id(self.get_host(bounced_node_info.node.account.ssh_hostname, admin_port), port, partition)
                    raise AssertionError("Network interruption failed. get_storage_max_transaction_id didn't return RemoteCommandError")
                except RemoteCommandError:
                    pass

            sleep(interrupt_duration)
        finally:
            # Step 5: Enable connection, Do this step even when the Step 4 fails, as the added iptable
            # rules aren't removed from VM with the end of this process
            for bounced_node_info in bounced_nodes:
                self.enable_traffic_to_port(bounced_node_info.node, port)

        # Step 6: Verify that total number of expected transactions matches number of transactions stored in waltz storage nodes
        for bounced_node_info in bounced_nodes:
            expected_number_of_transactions = (txn_per_client * num_clients) + bounced_node_info.total_num_of_transactions
            wait_until(lambda: expected_number_of_transactions == self.num_of_transactions_on_storage_node(bounced_node_info, admin_port, port, num_active_partitions),
                       timeout_sec=timeout, err_msg="number of transactions stored in storage partition does not match with all the transactions sent by client. "
                                             "Client {}, Strage = {} after {} seconds" \
                       .format(expected_number_of_transactions, self.num_of_transactions_on_storage_node(bounced_node_info, admin_port, port, num_active_partitions), timeout))

    def num_of_transactions_on_storage_node(self, bounced_node_info, admin_port, port, num_active_partitions):
        """
        :returns Total number of all transactions stored under active partitions in a storage node
        (i.e. (partition1 high watermark + 1) + (partition2 high watermark + 1) ...), where transactions starts at index -1
        """

        total_num_of_transaction = 0
        for partition in range(num_active_partitions):
            total_num_of_transaction += self.get_storage_max_transaction_id(self.get_host(bounced_node_info.node.account.ssh_hostname, admin_port),
                                                                            port, partition) + 1
        return total_num_of_transaction


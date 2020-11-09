from ducktape.mark.resource import cluster
from ducktape.mark import parametrize
from ducktape.cluster.cluster_spec import ClusterSpec
from waltz_ducktape.tests.produce_consume_validate import ProduceConsumeValidateTest
from waltz_ducktape.services.cli.server_cli import ServerCli
from ducktape.utils.util import wait_until
from random import randrange

class LoadRebalancingTest(ProduceConsumeValidateTest):
    """
        Class for tests simulating load rebalancing on server nodes
    """

    MIN_CLUSTER_SPEC = ClusterSpec.from_list([
        {'cpu': 1, 'mem': '1GB', 'disk': '25GB', 'additional_disks': {'/dev/sdb': '100GB'}, 'num_nodes': 3},
        {'cpu': 1, 'mem': '3GB', 'disk': '15GB', 'num_nodes': 2},
        {'cpu': 1, 'mem': '1GB', 'disk': '25GB', 'num_nodes': 1}])

    def __init__(self, test_context):
        super(LoadRebalancingTest, self).__init__(test_context=test_context)

    @cluster(cluster_spec=MIN_CLUSTER_SPEC)
    @parametrize(num_active_partitions=1, txn_per_client=150, num_clients=2, interval=600, timeout=360)
    @parametrize(num_active_partitions=4, txn_per_client=100, num_clients=2, interval=600, timeout=360)
    def test_rebalancing_on_server_nodes(self, num_active_partitions, txn_per_client, num_clients, interval, timeout):
        validation_cmd = self.client_cli.validate_txn_cmd(self.log_file_path, num_active_partitions, txn_per_client, num_clients, interval)
        self.run_produce_consume_validate(lambda: self.rebalancing_on_server_nodes(validation_cmd, timeout, num_active_partitions,
                                                                                   txn_per_client * num_clients))

    def rebalancing_on_server_nodes(self, validation_cmd, timeout, num_active_partitions, expected_number_of_transactions):
        """
        A validate function to simulate load rebalancing on server nodes using preferred-partitions.

        :param validation_cmd: Command that is passed to client node
        :param timeout: Test timeout
        :param num_active_partitions: number of active partitions
        :param expected_number_of_transactions: number of transactions expected to be stored on a waltz storage node
        """

        storage_admin_port = self.waltz_storage.admin_port
        server_port = self.waltz_server.port
        storage_port = self.waltz_storage.port
        storage_node = self.waltz_storage.nodes[randrange(len(self.waltz_storage.nodes))]
        random_active_partition = randrange(num_active_partitions)
        storage = self.get_host(storage_node.account.ssh_hostname, storage_admin_port)

        # Step 1: Start waltz cluster and execute validation command
        self.verifiable_client.start(validation_cmd)

        # Step 2: Wait till transactions get processed
        wait_until(lambda: self.is_max_transaction_id_updated(storage, storage_port, random_active_partition, -1), timeout_sec=timeout)

        # Step 3: Change assigned server node for random_active_partition
        partition_current_server_index = self.get_server_node_idx(random_active_partition)

        while True:
            partition_future_server_index = randrange(len(self.waltz_server.nodes))
            if not partition_current_server_index == partition_future_server_index:
                break

        server_node = self.waltz_server.nodes[partition_future_server_index]
        server_node_hostname = server_node.account.ssh_hostname
        server = self.get_host(server_node_hostname, server_port)
        server_cli = ServerCli(server_node, self.cli_config_path)
        server_cli.add_preferred_partition(server, random_active_partition)

        assert self.get_server_node_idx(random_active_partition) == partition_future_server_index, \
            "partition assignment for server nodes remained untouched"

        # Step 4: Wait until verifiable client ends its task
        wait_until(lambda: self.verifiable_client.task_complete() == True, timeout_sec=timeout,
                   err_msg="verifiable_client failed to complete task in {} seconds.".format(timeout))

        # Step 5: Assert all transactions are persistently stored.
        assert expected_number_of_transactions == self.get_storage_num_of_all_transactions(storage, storage_port, num_active_partitions), \
            "number of transactions stored in storage partition does not match with all the transactions sent by producers. " \
            "Client {}, Storage = {}" \
                .format(expected_number_of_transactions, self.get_storage_num_of_all_transactions(storage, storage_port, num_active_partitions))

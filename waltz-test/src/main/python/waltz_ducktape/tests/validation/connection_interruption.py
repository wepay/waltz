from waltz_ducktape.tests.produce_consume_validate import ProduceConsumeValidateTest
from ducktape.mark.resource import cluster
from ducktape.mark import parametrize
from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.utils.util import wait_until
from time import sleep
from random import randrange


class ConnectionInterruption(ProduceConsumeValidateTest):
    """
        Class for tests simulating network issues between server and storage.
    """

    MIN_CLUSTER_SPEC = ClusterSpec.from_list([
        {'cpu': 1, 'mem': '1GB', 'disk': '25GB', 'additional_disks': {'/dev/sdb': '100GB'}, 'num_nodes': 3},
        {'cpu': 1, 'mem': '3GB', 'disk': '15GB', 'num_nodes': 2},
        {'cpu': 1, 'mem': '1GB', 'disk': '25GB', 'num_nodes': 1}])

    def __init__(self, test_context):
        super(ConnectionInterruption, self).__init__(test_context=test_context)

    @cluster(cluster_spec=MIN_CLUSTER_SPEC)
    @parametrize(num_active_partitions=1, txn_per_client=100, num_clients=2, interval=600, timeout=360,
                 network_interruption_length=10, num_interruptions=3, duration_between_interruptions=5)
    @parametrize(num_active_partitions=4, txn_per_client=100, num_clients=2, interval=1000, timeout=300,
                 network_interruption_length=20, num_interruptions=1, duration_between_interruptions=20)
    def test_produce_consume_with_network_interruption(self, num_active_partitions, txn_per_client, num_clients, interval, timeout,
                                                network_interruption_length, num_interruptions, duration_between_interruptions):
        validation_cmd = self.client_cli.validate_txn_cmd(num_active_partitions, txn_per_client, num_clients, interval)
        node_idx = self.get_server_node_idx(randrange(min(num_active_partitions, len(self.waltz_server.nodes))))
        self.run_produce_consume_validate(lambda: self.produce_consume_with_network_interruption(validation_cmd, timeout, network_interruption_length, num_interruptions, duration_between_interruptions, node_idx, num_active_partitions))

    def produce_consume_with_network_interruption(self, validation_cmd, timeout, network_interruption_length, num_interruptions, duration_between_interruptions, node_idx, num_active_partitions):
        """
        Set up waltz and interrupt network between a waltz client node and a server node.

        :param validation_cmd: The command that is send to ClientCli
        :param timeout: Test timeout
        :param network_interruption_length: Interval in seconds during which client won't be able to connect to server
        :param num_interruptions: Number of connection interruption cycles
        :param duration_between_interruptions: Interval in seconds that represents duration between network interruptions
        :param node_idx: Index of a server node to which client won't be able to connect
        :param num_active_partitions: Number of active partitions
        """

        # Start waltz cluster and wait until a storage node registers first transaction
        self.verifiable_client.start(validation_cmd)
        admin_port = self.waltz_storage.admin_port
        port = self.waltz_storage.port
        wait_until(lambda: self.is_max_transaction_id_updated(self.get_host(self.waltz_storage.nodes[0].account.ssh_hostname, admin_port), port,
                                                       randrange(num_active_partitions), -1), timeout_sec=timeout)

        node = self.waltz_server.nodes[node_idx]
        for interruption in range(num_interruptions):
            sleep(duration_between_interruptions)

            # disable connection on port
            self.waltz_server.logger.info("Closing a port")
            node.account.ssh_capture(
                "sudo iptables -I INPUT -p tcp --destination-port {} -j DROP".format(self.waltz_server.port))
            sleep(network_interruption_length)

            # enable connection on port
            self.waltz_server.logger.info("Opening a port")
            node.account.ssh_capture(
                "sudo iptables -D INPUT -p tcp --destination-port {} -j DROP".format(self.waltz_server.port))

        wait_until(lambda: self.verifiable_client.task_complete() == True, timeout_sec=timeout,
                   err_msg="verifiable_client failed to complete task in %d seconds." % timeout)

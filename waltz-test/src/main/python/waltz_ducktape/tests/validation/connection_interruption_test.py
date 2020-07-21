from waltz_ducktape.tests.produce_consume_validate import ProduceConsumeValidateTest
from ducktape.mark.resource import cluster
from ducktape.mark import parametrize
from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.utils.util import wait_until
from time import sleep
from random import randrange


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
    @parametrize(num_active_partitions=1, txn_per_client=100, num_clients=2, interval=600, timeout=360,
                 interruption_duration=10, num_interruptions=3, delay_between_interruptions=5)
    @parametrize(num_active_partitions=4, txn_per_client=100, num_clients=2, interval=1000, timeout=300,
                 interruption_duration=20, num_interruptions=1, delay_between_interruptions=20)
    def test_produce_consume_with_network_interruption(self, num_active_partitions, txn_per_client, num_clients, interval, timeout,
                                                interruption_duration, num_interruptions, delay_between_interruptions):
        validation_cmd = self.client_cli.validate_txn_cmd(num_active_partitions, txn_per_client, num_clients, interval)
        self.run_produce_consume_validate(lambda: self.produce_consume_with_network_interruption(validation_cmd, timeout, interruption_duration, num_interruptions, delay_between_interruptions, num_active_partitions, interval))

    def drop_traffic_to_port(self, node, port):
        node.account.ssh_capture("sudo iptables -I INPUT -p tcp --destination-port {} -j DROP".format(port))

    def enable_traffic_to_port(self, node, port):
        node.account.ssh_capture("sudo iptables -D INPUT -p tcp --destination-port {} -j DROP".format(port))

    def produce_consume_with_network_interruption(self, validation_cmd, timeout, interruption_duration, num_interruptions, delay_between_interruptions, num_active_partitions, interval):
        """
        Set up waltz and interrupt network between a waltz client node and a server node.

        :param validation_cmd: The command that is send to ClientCli
        :param timeout: Test timeout
        :param interruption_duration: Interval in seconds during which client won't be able to connect to server
        :param num_interruptions: Number of connection interruption cycles
        :param delay_between_interruptions: Interval in seconds that represents duration between network interruptions
        :param num_active_partitions: Number of active partitions
        :param interval: Average interval(millisecond) between transactions
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
            # disable connection on port
            self.drop_traffic_to_port(node, self.waltz_server.port)

            #verify that new transactions aren't processed
            sleep(interval)
            cur_high_watermark = self.get_storage_max_transaction_id(storage, port, partition)
            sleep(max(interruption_duration - interval, 0))
            if self.is_max_transaction_id_updated(storage, port, partition, cur_high_watermark):
                # delete added iptable rule as it is not removed with the end of waltz process
                self.enable_traffic_to_port(node, self.waltz_server.port)
                raise Exception('Network interruption failed')

            # enable connection on port
            self.enable_traffic_to_port(node, self.waltz_server.port)

        wait_until(lambda: self.verifiable_client.task_complete() == True, timeout_sec=timeout,
                   err_msg="verifiable_client failed to complete task in %d seconds." % timeout)

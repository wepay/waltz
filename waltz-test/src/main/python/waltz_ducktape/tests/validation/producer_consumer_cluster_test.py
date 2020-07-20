from ducktape.mark.resource import cluster
from ducktape.mark import parametrize
from ducktape.cluster.cluster_spec import ClusterSpec
from waltz_ducktape.tests.produce_consume_validate import ProduceConsumeValidateTest
from ducktape.utils.util import wait_until


class ProducerConsumerClusterTest(ProduceConsumeValidateTest):
    """
    A class of torture tests that turns on a bunch of ZK, storage, server,
    and client nodes. Each client is an independent process which makes it easier
    for client torture testing.
    """
    MIN_CLUSTER_SPEC = ClusterSpec.from_list([
        {'cpu':1, 'mem':'1GB', 'disk':'25GB', 'additional_disks':{'/dev/sdb':'100GB'}, 'num_nodes':3},
        {'cpu':1, 'mem':'3GB', 'disk':'15GB', 'num_nodes':2},
        {'cpu':1, 'mem':'1GB', 'disk':'25GB', 'num_nodes':1}])

    def __init__(self, test_context):
        super(ProducerConsumerClusterTest, self).__init__(test_context=test_context)

    @cluster(cluster_spec=MIN_CLUSTER_SPEC)
    @parametrize(num_active_partitions=1, txn_per_client=75, num_producers=3, num_consumers=2, interval=250, timeout=360)
    @parametrize(num_active_partitions=4, txn_per_client=75, num_producers=2, num_consumers=2, interval=500, timeout=360)
    def test_produce_consume_no_torture(self, num_active_partitions, txn_per_client, num_producers, num_consumers, interval, timeout):
        self.run_produce_consume_validate(lambda: self.setup_client_side(num_active_partitions, txn_per_client, num_producers, num_consumers, interval, timeout))



    def setup_client_side(self, num_active_partitions, txn_per_client, num_producers, num_consumers, interval, timeout):
        previous_high_watermark = self.get_watermark_for_active_partitions(num_active_partitions)
        validation_producer_consumer_cluster = self.client_cli.validate_consumer_producer_cluster_cmd(num_active_partitions, txn_per_client, num_producers, num_consumers, interval, previous_high_watermark)

        self.verifiable_client.start(validation_producer_consumer_cluster)

        wait_until(lambda: self.verifiable_client.task_complete() == True, timeout_sec=timeout,
                   err_msg="verifiable_client failed to complete task in %d seconds." % timeout)

    def get_watermark_for_active_partitions(self, num_active_partitions):
        watermarks = []
        admin_port = self.waltz_storage.admin_port
        port = self.waltz_storage.port

        storage_node_hostname = self.waltz_storage.nodes[0].account.ssh_hostname
        for partition in range(num_active_partitions):
            watermark = self.get_storage_max_transaction_id(self.get_host(storage_node_hostname, admin_port), port, partition, False)
            watermarks.append(max(watermark, -1))
        return watermarks


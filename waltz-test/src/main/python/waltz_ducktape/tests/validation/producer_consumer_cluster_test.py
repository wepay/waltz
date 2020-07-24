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
        validation_cmd = self.client_cli.validate_consumer_producer_cluster_cmd(num_active_partitions, txn_per_client, num_producers, num_consumers, interval)
        self.run_produce_consume_validate(lambda: self.simple_validation_func(validation_cmd, timeout));


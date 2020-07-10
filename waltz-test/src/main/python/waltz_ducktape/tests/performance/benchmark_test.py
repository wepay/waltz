from re import search
from ducktape.mark.resource import cluster
from ducktape.mark import parametrize
from ducktape.cluster.cluster_spec import ClusterSpec
from waltz_ducktape.tests.produce_consume_validate import ProduceConsumeValidateTest


class BenchmarkTest(ProduceConsumeValidateTest):
    """
    A benchmark of Waltz producer/consumer performance.
    """
    MIN_CLUSTER_SPEC = ClusterSpec.from_list([
        {'cpu':1, 'mem':'1GB', 'disk':'25GB', 'additional_disks':{'/dev/sdb':'100GB'}, 'num_nodes':3},
        {'cpu':1, 'mem':'3GB', 'disk':'15GB', 'num_nodes':2},
        {'cpu':1, 'mem':'1GB', 'disk':'25GB', 'num_nodes':1}])

    def __init__(self, test_context):
        super(BenchmarkTest, self).__init__(test_context=test_context)

    @cluster(cluster_spec=MIN_CLUSTER_SPEC)
    @parametrize(txn_size=512, txn_per_thread=1000, num_thread=10, interval=10, lock_pool_size=0, num_active_partitions=1, timeout=420, up_to_date_watermark=None)
    @parametrize(txn_size=512, txn_per_thread=1000, num_thread=10, interval=10, lock_pool_size=0, num_active_partitions=1, timeout=420, up_to_date_watermark=True)
    @parametrize(txn_size=512, txn_per_thread=1000, num_thread=100, interval=20, lock_pool_size=0, num_active_partitions=1, timeout=420, up_to_date_watermark=None)
    @parametrize(txn_size=512, txn_per_thread=2000, num_thread=50, interval=10, lock_pool_size=0, num_active_partitions=1, timeout=420, up_to_date_watermark=None)
    @parametrize(txn_size=1024, txn_per_thread=1000, num_thread=100, interval=10, lock_pool_size=0, num_active_partitions=1, timeout=420, up_to_date_watermark=None)
    @parametrize(txn_size=512, txn_per_thread=100, num_thread=100, interval=10, lock_pool_size=64, num_active_partitions=1, timeout=420, up_to_date_watermark=None)
    @parametrize(txn_size=512, txn_per_thread=100, num_thread=100, interval=10, lock_pool_size=128, num_active_partitions=1, timeout=420, up_to_date_watermark=None)
    @parametrize(txn_size=512, txn_per_thread=100, num_thread=100, interval=10, lock_pool_size=128, num_active_partitions=2, timeout=420, up_to_date_watermark=None)
    def test_producer_performance(self, txn_size, txn_per_thread, num_thread, interval, lock_pool_size, num_active_partitions, timeout, up_to_date_watermark):
        test_cmd = self.performance_cli.producer_test_cmd(txn_size, txn_per_thread, num_thread, interval, lock_pool_size, num_active_partitions, up_to_date_watermark)
        test_output = self.run_produce_consume_validate(lambda: self.simple_validation_func(test_cmd, timeout))
        self.print_producer_performance(test_output)

    @cluster(cluster_spec=MIN_CLUSTER_SPEC)
    @parametrize(txn_size=512, num_txn=100000, num_active_partitions=1, timeout=420, up_to_date_watermark=True)
    @parametrize(txn_size=512, num_txn=100000, num_active_partitions=4, timeout=420, up_to_date_watermark=None)
    @parametrize(txn_size=1024, num_txn=100000, num_active_partitions=1, timeout=420, up_to_date_watermark=None)
    def test_consumer_performance(self, txn_size, num_txn, num_active_partitions, timeout, up_to_date_watermark):
        test_cmd = self.performance_cli.consumer_test_cmd(txn_size, num_txn, num_active_partitions, up_to_date_watermark)
        test_output = self.run_produce_consume_validate(lambda: self.simple_validation_func(test_cmd, timeout))
        self.print_consumer_performance(test_output)

    def print_producer_performance(self, test_output):
        performance = search(".*transactions(.|\n)*MilliSec\/Transaction.*", test_output).group(0)
        print("\n####################### PRODUCER PERFORMANCE REPORT #######################\n" + \
              "\n{performance}\n".format(performance=performance) + \
              "\n###########################################################################\n")

    def print_consumer_performance(self, test_output):
        performance = search(".*transactions(.|\n)*MB/sec.*", test_output).group(0)
        print("\n####################### CONSUMER PERFORMANCE REPORT #######################\n" + \
              "\n{performance}\n".format(performance=performance) + \
              "\n###########################################################################\n")

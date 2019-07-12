from re import search
from ducktape.mark.resource import cluster
from ducktape.mark import parametrize
from waltz_ducktape.tests.produce_consume_validate import ProduceConsumeValidateTest


class BenchmarkTest(ProduceConsumeValidateTest):
    """
    A benchmark of Waltz producer/consumer performance.
    """
    def __init__(self, test_context):
        super(BenchmarkTest, self).__init__(test_context=test_context)

    @cluster(nodes_spec={'storage':3, 'server':2, 'client':1})
    @parametrize(txn_size=512, txn_per_thread=1000000, num_thread=10, interval=0, lock_pool_size=0, timeout=3600)
    def test_producer_performance(self, txn_size, txn_per_thread, num_thread, interval, lock_pool_size, timeout):
        test_cmd = self.performance_cli.producer_test_cmd(txn_size, txn_per_thread, num_thread, interval, lock_pool_size)
        test_result = self.run_produce_consume_validate(lambda: self.simple_validation_func(test_cmd, timeout))
        self._print_report(test_result)

    @cluster(nodes_spec={'storage':3, 'server':2, 'client':1})
    @parametrize(txn_size=512, num_txn=1000000, timeout=3600)
    def test_consumer_performance(self, txn_size, num_txn, timeout):
        test_cmd = self.performance_cli.consumer_test_cmd(txn_size, num_txn)
        test_result = self.run_produce_consume_validate(lambda: self.simple_validation_func(test_cmd, timeout))
        self._print_report(test_result)

    def _print_report(self, test_result):
        performance = search(".*transactions(.|\n)*milliSec\/Transaction:.*", test_result).group(0)
        report = """
        \n####################### PERFORMANCE REPORT #######################
        \n{performance}
        \n##################################################################
        """.format(performance=performance)
        print(report)

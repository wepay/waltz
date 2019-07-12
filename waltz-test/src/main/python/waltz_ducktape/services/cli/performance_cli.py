from waltz_ducktape.services.cli.base_cli import Cli


class PerformanceCli(Cli):
    """
    PerformanceCli is an utility class to interact with com.wepay.waltz.tools.performance.PerformanceCli.
    """
    def __init__(self, cli_config_path):
        """
        Construct a new 'PerformanceCli' object.

        :param cli_config_path: The path to cli config file
        """
        super(PerformanceCli, self).__init__(cli_config_path)

    def producer_test_cmd(self, txn_size, txn_per_thread, num_thread, interval, lock_pool_size=None):
        """
        Return producer performance test command:

        java com.wepay.waltz.tools.performance.PerformanceCli \
            test-producers \
            <full_path_config_file> \
            --txn-size <size of each transaction> \
            --txn-per-thread <number of transactions per thread> \
            --num-thread <number of total threads> \
            --interval <average interval(millisecond) between transactions> \
            --lock-pool-size <size of lock pool>
        """
        cmd_arr = [
            "java", self.java_cli_class_name(),
            "test-producers",
            self.cli_config_path,
            "--txn-size", txn_size,
            "--txn-per-thread", txn_per_thread,
            "--num-thread", num_thread,
            "--interval", interval,
            "--lock-pool-size {}".format(lock_pool_size) if lock_pool_size is not None else ""
        ]
        return self.build_cmd(cmd_arr)

    def consumer_test_cmd(self, txn_size, num_txn):
        """
        Return consumer performance test command:

        java com.wepay.waltz.tools.performance.PerformanceCli \
            test-consumers \
            <full_path_config_file> \
            --txn-size <size of each transaction> \
            --num-txn <number of transactions to send> \
        """
        cmd_arr = [
            "java", self.java_cli_class_name(),
            "test-consumers",
            self.cli_config_path,
            "--txn-size", txn_size,
            "--num-txn", num_txn
        ]
        return self.build_cmd(cmd_arr)

    def java_cli_class_name(self):
        return "com.wepay.waltz.tools.performance.PerformanceCli"

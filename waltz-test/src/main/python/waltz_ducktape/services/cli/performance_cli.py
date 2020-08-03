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

    def producer_test_cmd(self, txn_size, txn_per_thread, num_thread, interval, lock_pool_size=None, num_active_partitions=None, mount_from_latest=None):
        """
        Return producer performance test command:

        java com.wepay.waltz.tools.performance.PerformanceCli \
            test-producers \
            --txn-size <size of each transaction> \
            --txn-per-thread <number of transactions per thread> \
            --num-thread <number of total threads> \
            --interval <average interval(millisecond) between transactions> \
            --cli-config-path <the path to cli config file> \
            --lock-pool-size <size of lock pool> \
            --num-active-partitions <number of partitions to interact with>
            --mount_from_latest Waltz Client would be mounted from the latest HighWaterMark (for a partition) on Waltz
        """
        cmd_arr = [
            "java -Dlog4j.configuration=file:/etc/waltz-client/waltz-log4j.cfg", self.java_cli_class_name(),
            "test-producers",
            "--txn-size", txn_size,
            "--txn-per-thread", txn_per_thread,
            "--num-thread", num_thread,
            "--interval", interval,
            "--cli-config-path", self.cli_config_path,
            "--lock-pool-size {}".format(lock_pool_size) if lock_pool_size is not None else "",
            "--num-active-partitions {}".format(num_active_partitions) if num_active_partitions is not None else "",
            "--mount_from_latest" if mount_from_latest is not None else ""
        ]
        return self.build_cmd(cmd_arr)

    def consumer_test_cmd(self, txn_size, num_txn, num_active_partitions=None, mount_from_latest=None):
        """
        Return consumer performance test command:

        java com.wepay.waltz.tools.performance.PerformanceCli \
            test-consumers \
            --txn-size <size of each transaction> \
            --num-txn <number of transactions to send> \
            --cli-config-path <the path to cli config file> \
            --num-active-partitions <number of active partitions>
            --mount_from_latest Waltz Client would be mounted from the latest HighWaterMark (for a partition) on Waltz
        """
        cmd_arr = [
            "java -Dlog4j.configuration=file:/etc/waltz-client/waltz-log4j.cfg", self.java_cli_class_name(),
            "test-consumers",
            "--txn-size", txn_size,
            "--num-txn", num_txn,
            "--cli-config-path", self.cli_config_path,
            "--num-active-partitions {}".format(num_active_partitions) if num_active_partitions is not None else "",
            "--mount_from_latest" if mount_from_latest is not None else ""
        ]
        return self.build_cmd(cmd_arr)

    def java_cli_class_name(self):
        return "com.wepay.waltz.tools.performance.PerformanceCli"

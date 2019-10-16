from waltz_ducktape.services.cli.base_cli import Cli


class ClientCli(Cli):
    """
    ClientCli is an utility class to interact with com.wepay.waltz.tools.client.ClientCli.
    """
    def __init__(self, cli_config_path):
        """
        Construct a new 'ClientCli' object.

        :param cli_config_path: The path to client cli config file
        """
        super(ClientCli, self).__init__(cli_config_path)

    def validate_txn_cmd(self, num_active_partitions, txn_per_client, num_clients, interval, high_watermark=None):
        """
        Return validation cli command to submit and validate transactions, which
        includes validating high water mark, transaction data and optimistic lock.

        java com.wepay.waltz.tools.client.ClientCli \
            validate \
            --txn-per-client <number of transactions per client> \
            --num-clients <number of total clients> \
            --interval <average interval(millisecond) between transactions> \
            --cli-config-path <client cli config file path> \
            --high-watermark <current high watermark for the partition, default to -1> \
            --num-active-partitions <number of partitions to interact with>
        """
        cmd_arr = [
            "java -Dlog4j.configuration=file:/etc/waltz-client/waltz-log4j.cfg", self.java_cli_class_name(),
            "validate",
            "--txn-per-client", txn_per_client,
            "--num-clients", num_clients,
            "--interval", interval,
            "--cli-config-path", self.cli_config_path,
            "--high-watermark {}".format(high_watermark) if high_watermark is not None else "",
            "--num-active-partitions {}".format(num_active_partitions) if num_active_partitions is not None else ""
        ]
        return self.build_cmd(cmd_arr)

    def java_cli_class_name(self):
        return "com.wepay.waltz.tools.client.ClientCli"

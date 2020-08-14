from waltz_ducktape.services.cli.base_cli import Cli


class ServerCli(Cli):
    """
    ServerCli is an utility class to interact with com.wepay.waltz.tools.server.ServerCli.
    """
    def __init__(self, node, cli_config_path):
        """
        Construct a new 'ServerCli' object.

        :param node: The node to run cli command
        """
        super(ServerCli, self).__init__(cli_config_path)
        self.node = node

    def list_partition(self, server):
        """
        Runs this command to return Waltz server partition information:

        java com.wepay.waltz.tools.server.ServerCli \
            list \
            --server <server with jetty port, in format of host:port>
        """
        cmd_arr = [
            "java", self.java_cli_class_name(),
            "list",
            "--server", server
        ]

        return self.node.account.ssh_output(self.build_cmd(cmd_arr))

    def add_preferred_partition(self, server, partition):
        """
        Runs this command to add a preferred partition to the server

        java com.wepay.waltz.tools.server.ServerCli \
            add-preferred-partition \
            --server <server with jetty port, in format of host:port>
            --add-preferred-partition <partition number>
            --cli-config-path <client cli config file path>
        """
        cmd_arr = [
            "java", self.java_cli_class_name(),
            "add-preferred-partition",
            "--server", server,
            "--partition", partition,
            "--cli-config-path", self.cli_config_path
        ]
        return self.node.account.ssh_output(self.build_cmd(cmd_arr))

    def java_cli_class_name(self):
        return "com.wepay.waltz.tools.server.ServerCli"

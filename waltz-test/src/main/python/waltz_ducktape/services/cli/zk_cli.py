from waltz_ducktape.services.cli.base_cli import Cli


class ZkCli(Cli):
    """
    ZkCli is an utility class to interact with com.wepay.waltz.tools.zk.ZookeeperCli.
    """
    def __init__(self, node, cli_config_path):
        """
        Construct a new 'ZkCli' object.

        :param node: The node to run cli command
        :param cli_config_path: The path to cli config file
        """
        super(ZkCli, self).__init__(cli_config_path)
        self.node = node

    def list_zookeeper_metadata(self):
        """
        Runs this command to return ZooKeeper metadata to the console:

        java com.wepay.waltz.tools.zk.ZooKeeperCli \
            list \
            --cli-config-path <the path to cli config file>
        """
        cmd_arr = [
            "java", self.java_cli_class_name(),
            "list",
            "--cli-config-path", self.cli_config_path,
        ]

        return self.node.account.ssh_output(self.build_cmd(cmd_arr))

    def create_cluster(self, name, partitions):
        """
        Runs this command to create a new Waltz cluster in ZooKeeper:

        java com.wepay.waltz.tools.zk.ZooKeeperCli \
            create \
            --name <cluster name> \
            --partitions <number of partitions> \
            --cli-config-path <the path to cli config file>
        """
        cmd_arr = [
            "java", self.java_cli_class_name(),
            "create",
            "--name", name,
            "--partitions", partitions,
            "--cli-config-path", self.cli_config_path,
        ]

        self.node.account.ssh(self.build_cmd(cmd_arr))

    def delete_cluster(self, name, force=None):
        """
        Runs this command to delete an existing Waltz cluster in ZooKeeper:

        java com.wepay.waltz.tools.zk.ZooKeeperCli \
            delete \
            --name <cluster name> \
            --force <the flag to delete cluster even name doesn't match> \
            --cli-config-path <the path to cli config file>
        """
        cmd_arr = [
            "java", self.java_cli_class_name(),
            "delete",
            "--name", name,
            "--force" if force else "",
            "--cli-config-path", self.cli_config_path
        ]

        self.node.account.ssh(self.build_cmd(cmd_arr))

    def add_storage_node(self, storage, storage_admin_port, group):
        """
        Runs this command to add a storage node to cluster:

        java com.wepay.waltz.tools.zk.ZooKeeperCli \
            add-storage-node \
            --storage <the storage node to be added to, in format of host:port> \
            --storage-admin-port <the admin port of the storage node to add> \
            --group <the group to add to> \
            --cli-config-path <the path to cli config file>
        """
        cmd_arr = [
            "java", self.java_cli_class_name(),
            "add-storage-node",
            "--storage", storage,
            "--storage-admin-port", storage_admin_port,
            "--group", group,
            "--cli-config-path", self.cli_config_path
        ]

        self.node.account.ssh(self.build_cmd(cmd_arr))

    def remove_storage_node(self, storage):
        """
        Runs this command to remove a storage node from cluster:

        java com.wepay.waltz.tools.zk.ZooKeeperCli \
            remove-storage-node \
            --storage <the storage node to be removed, in format of host:port> \
            --cli-config-path <the path to cli config file>
        """
        cmd_arr = [
            "java", self.java_cli_class_name(),
            "remove-storage-node",
            "--storage", storage,
            "--cli-config-path", self.cli_config_path
        ]

        self.node.account.ssh(self.build_cmd(cmd_arr))

    def assign_partition(self, partition, storage):
        """
        Runs this command to assign a partition to a storage node:

        java com.wepay.waltz.tools.zk.ZooKeeperCli \
            assign-partition \
            --partition <the partition to assign> \
            --storage <the storage node to be assigned to, in format of host:port> \
            --cli-config-path <the path to cli config file>
        """
        cmd_arr = [
            "java", self.java_cli_class_name(),
            "assign-partition",
            "--partition", partition,
            "--storage", storage,
            "--cli-config-path", self.cli_config_path
        ]

        self.node.account.ssh(self.build_cmd(cmd_arr))

    def unassign_partition(self, partition, storage):
        """
        Runs this command to un-assign a partition from a storage node:

        java com.wepay.waltz.tools.zk.ZooKeeperCli \
            unassign-partition \
            --partition <the partition to assign> \
            --storage <the storage node to be un-assigned from, in format of host:port> \
            --cli-config-path <the path to cli config file>
        """
        cmd_arr = [
            "java", self.java_cli_class_name(),
            "unassign-partition",
            "--partition", partition,
            "--storage", storage,
            "--cli-config-path", self.cli_config_path
        ]

        self.node.account.ssh(self.build_cmd(cmd_arr))

    def auto_assign(self, group):
        """
        Runs this command to automatically assign partitions to storage nodes in group:

        java com.wepay.waltz.tools.zk.ZooKeeperCli \
            auto-assign \
            --group <the group to assign to> \
            --cli-config-path <the path to cli config file>
        """
        cmd_arr = [
            "java", self.java_cli_class_name(),
            "auto-assign",
            "--group", group,
            "--cli-config-path", self.cli_config_path
        ]

        self.node.account.ssh(self.build_cmd(cmd_arr))

    def java_cli_class_name(self):
        return "com.wepay.waltz.tools.zk.ZooKeeperCli"

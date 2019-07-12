from waltz_ducktape.services.cli.base_cli import Cli


class StorageCli(Cli):
    """
    StorageCli is an utility class to interact with com.wepay.waltz.tools.storage.StorageCli.
    """
    def __init__(self, node, cli_config_path):
        """
        Construct a new 'StorageCli' object.

        :param node: The node to run cli command
        :param cli_config_path: The path to cli config file
        """
        super(StorageCli, self).__init__(cli_config_path)
        self.node = node

    def list_partition(self, storage):
        """
        Runs this command to return partition ownership data of a storage node:

        java com.wepay.waltz.tools.storage.StorageCli \
            list \
            --storage <in format of host:port, where port is the admin port>
        """
        cmd_arr = [
            "java", self.java_cli_class_name(),
            "list",
            "--storage", storage,
            "--cli-config-path", self.cli_config_path
        ]

        return self.node.account.ssh_output(self.build_cmd(cmd_arr))

    def add_partition(self, storage, partition):
        """
        Runs this command to add a partition ownership to a storage node:

        java com.wepay.waltz.tools.storage.StorageCli \
            add-partition \
            --storage <in format of host:port, where port is the admin port> \
            --partition <the partition id to be added to the storage node> \
            --cli-config-path <the path to cli config file>
        """
        cmd_arr = [
            "java", self.java_cli_class_name(),
            "add-partition",
            "--storage", storage,
            "--partition", partition,
            "--cli-config-path", self.cli_config_path
        ]

        self.node.account.ssh(self.build_cmd(cmd_arr))

    def remove_partition(self, storage, partition, delete_storage_files=None):
        """
        Runs this command to remove a partition ownership from a storage node:

        java com.wepay.waltz.tools.storage.StorageCli \
            remove-partition \
            --storage <in format of host:port, where port is the admin port> \
            --partition <the partition id to be removed from the storage node> \
            --cli-config-path <the path to cli config file>
        """
        cmd_arr = [
            "java", self.java_cli_class_name(),
            "remove-partition",
            "--storage", storage,
            "--partition", partition,
            "--cli-config-path", self.cli_config_path,
            "--delete-storage-files" if delete_storage_files else ""
        ]
        self.node.account.ssh(self.build_cmd(cmd_arr))

    def availability(self, storage, partition, availability=None):
        """
        Runs this command to set the read/write availability the partition in a storage node:

        java com.wepay.waltz.tools.storage.StorageCli \
            availability \
            --storage <in format of host:port, where port is the admin port> \
            --partition <the partition id to be added to the storage node> \
            --availability <'offline' or 'online' for the storage node> \
            --cli-config-path <the path to cli config file>
        """
        cmd_arr = [
            "java", self.java_cli_class_name(),
            "availability",
            "--storage", storage,
            "--partition", partition,
            "--availability {}".format(availability) if availability is not None else "",
            "--cli-config-path", self.cli_config_path
        ]

        self.node.account.ssh(self.build_cmd(cmd_arr))

    def recover_partition(self, source_storage, destination_storage, partition, batch_size,
                          source_ssl_config_path=None, destination_ssl_config_path=None):
        """
        Runs this command to load data from a partition into a storage node:

        java com.wepay.waltz.tools.storage.StorageCli \
            recover-partition \
            --source-storage <in format of host:port, where port is the admin port> \
            --destination-storage <in format of host:port, where port is the admin port> \
            --partition <the partition id to be removed from the storage node> \
            --batch-size <the batch size to use when fetching records from storage node> \
            --cli-config-path <the path to cli config file> \
            --source-ssl-config-path <the SSL config file path required for the source storage node> \
            --destination-ssl-config-path <the SSL config file path required for the destination storage node>
        """
        cmd_arr = [
            "java", self.java_cli_class_name(),
            "recover-partition",
            "--source-storage", source_storage,
            "--destination-storage", destination_storage,
            "--partition", partition,
            "--batch-size", batch_size,
            "--cli-config-path", self.cli_config_path,
            "--source-ssl-config-path {}".format(source_ssl_config_path) if source_ssl_config_path is not None else "",
            "--destination-ssl-config-path {}".format(destination_ssl_config_path) if destination_ssl_config_path is not None else ""
        ]

        self.node.account.ssh(self.build_cmd(cmd_arr))

    def sync_partition_assignments(self):
        """
        Runs this command to sync partition ownership to storage nodes based on assignment specified in ZooKeeper:

        java com.wepay.waltz.tools.storage.StorageCli \
            sync-partitions \
            --cli-config-path <the path to cli config file> \
        """
        cmd_arr = [
            "java", self.java_cli_class_name(),
            "sync-partitions",
            "--cli-config-path", self.cli_config_path,
        ]

        self.node.account.ssh(self.build_cmd(cmd_arr))

    def java_cli_class_name(self):
        return "com.wepay.waltz.tools.storage.StorageCli"

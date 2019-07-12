from re import search
from ducktape.utils.util import wait_until
from waltz_ducktape.tests.waltz_test import WaltzTest
from waltz_ducktape.services.cli.zk_cli import ZkCli
from waltz_ducktape.services.cli.storage_cli import StorageCli
from waltz_ducktape.services.cli.client_cli import ClientCli
from waltz_ducktape.services.cli.performance_cli import PerformanceCli


class ProduceConsumeValidateTest(WaltzTest):
    """
    This class provides a shared template for tests which follow the common pattern of:

        - produce transactions in the background
        - consume transactions in the background
        - torture the system, e.g. bounce service to trigger recovery procedure etc.
        - perform validation
    """
    def __init__(self, test_context, cluster_num_partitions=None, num_storage_nodes=None,
                 num_server_nodes=None, num_client_nodes=None):
        """
        Construct a new 'ProduceConsumeValidateTest' object.

        :param test_context: The test context
        """
        super(ProduceConsumeValidateTest, self).__init__(test_context=test_context)

        # initialize waltz services
        self.num_storage_nodes = num_storage_nodes
        self.num_server_nodes = num_server_nodes
        self.num_client_nodes = num_client_nodes
        self.waltz_server = self.get_server_service(cluster_num_partitions, self.num_server_nodes)
        self.verifiable_client = self.get_verifiable_client(self.num_client_nodes)
        # waltz storage will be initiated in reset_cluster()
        # because cluster key is generated after created cluster
        self.waltz_storage = None

        # initialize waltz cli instances
        self.admin_node = self.waltz_server.get_admin_node()
        self.cli_config_path = self.waltz_server.cli_config_file_path()
        self.client_config_path = self.verifiable_client.config_file_path()
        self.zk_cli = ZkCli(self.admin_node, self.cli_config_path)
        self.storage_cli = StorageCli(self.admin_node, self.cli_config_path)
        self.client_cli = ClientCli(self.client_config_path)
        self.performance_cli = PerformanceCli(self.client_config_path)

        # a list of storage nodes to skip during setup
        self.storage_nodes_to_skip = []

    def run_produce_consume_validate(self, validation_func, torture_func=None):
        """
        Top-level template for simple produce/consume/validate tests.
        """
        try:
            self.setup_waltz()

            if torture_func is not None:
                torture_func()

            validation_result = validation_func()

            return validation_result
        except BaseException:
            for s in self.test_context.services:
                self.mark_for_collect(s)
            raise

    def setup_waltz(self):
        """
        Setup Waltz cluster for client to interact with.
        """
        # Step 1: ZkCli - create waltz cluster.
        self.reset_cluster()

        # Step 2: ZkCli - add storage nodes to cluster, one in each group.
        storage_nodes_to_setup = [node for idx, node in enumerate(self.waltz_storage.nodes) if idx not in self.storage_nodes_to_skip]
        for idx, node in enumerate(storage_nodes_to_setup):
            hostname = node.account.ssh_hostname
            port = self.waltz_storage.port
            admin_port = self.waltz_storage.admin_port
            self.zk_add_storage_node(storage=self.get_host(hostname, port), storage_admin_port=admin_port, group=idx)

        # Step 3: ZkCli - auto assign partitions to storage nodes in the group.
        # Each replica will be assigned with all partitions.
        for idx in range(len(storage_nodes_to_setup)):
            self.zk_auto_assign_partitions(group=idx)

        # Step 4: WaltzStorage - start storage instances.
        self.waltz_storage.start()

        # Step 5: StorageCli - synchronize partition ownership to storage nodes.
        # based on assignment specified in ZooKeeper
        self.storage_sync_partition_assignments()

        # Step 6: StorageCli - set storage nodes readable and writable.
        for node in storage_nodes_to_setup:
            hostname = node.account.ssh_hostname
            admin_port = self.waltz_storage.admin_port
            self.storage_set_availability(storage=self.get_host(hostname, admin_port), partition=0, availability="online")

        # Step 7: WaltzServer - start server instances.
        self.waltz_server.start()

    def reset_cluster(self):
        """
        Re-create zookeeper cluster before test start.
        """
        cluster_name = self.waltz_server.cluster_name
        cluster_num_partitions = self.waltz_server.cluster_num_partitions

        self.logger.info("Deleting Waltz cluster: {}".format(cluster_name))
        self.zk_cli.delete_cluster(cluster_name)

        self.logger.info("Creating Waltz cluster: {}".format(cluster_name))
        self.zk_cli.create_cluster(cluster_name, cluster_num_partitions)

        cluster_key = self.get_cluster_key()
        self.waltz_storage = self.get_storage_service(cluster_key, cluster_num_partitions, self.num_storage_nodes)

    def stop_waltz_client(self):
        self.verifiable_client.stop()
        self.verifiable_client.clean()

    def simple_validation_func(self, validation_cmd, timeout):
        """
        A simple validation function that runs a single validation command
        with verifiable_client.
        :return: validation result.
        """
        self.verifiable_client.start(validation_cmd)

        wait_until(lambda: self.verifiable_client.task_complete() == True, timeout_sec=timeout,
                   err_msg="verifiable_client failed to complete task in {} seconds.".format(timeout))

        self.verifiable_client.stop()
        return self.verifiable_client.get_validation_result()

    def zk_add_storage_node(self, storage, storage_admin_port, group):
        self.logger.info("Adding storage node {} to group {}".format(storage, group))
        self.zk_cli.add_storage_node(storage, storage_admin_port, group)

    def zk_auto_assign_partitions(self, group):
        self.logger.info("Auto-assigning partitions to storage nodes in group {}".format(group))
        self.zk_cli.auto_assign(group)

    def zk_assign_partition(self, storage, partition):
        self.logger.info("Assigning partition {} to storage node {}".format(partition, storage))
        self.zk_cli.assign_partition(partition, storage)

    def storage_add_partition(self, storage, partition):
        self.logger.info("Adding partition {} to storage node {}".format(partition, storage))
        self.storage_cli.add_partition(storage, partition)

    def storage_set_availability(self, storage, partition, availability=None):
        self.logger.info("Setting the partition {} read/write availability of storage node {}".format(partition, storage))
        self.storage_cli.availability(storage, partition, availability)

    def storage_sync_partition_assignments(self):
        self.logger.info("Synchronizing partition ownership to storage nodes based on assignment in ZooKeeper")
        self.storage_cli.sync_partition_assignments()

    def storage_recover_partition(self, source_storage, destination_storage, partition, batch_size):
        self.logger.info("Loading partition {} data from storage {} to a storage {}".format(partition, source_storage, destination_storage))
        source_ssl_config_path = self.waltz_server.service_config_file_path()
        destination_ssl_config_path = source_ssl_config_path
        self.storage_cli.recover_partition(source_storage, destination_storage, partition, batch_size,
                                           source_ssl_config_path, destination_ssl_config_path)

    def get_storage_local_low_watermark(self, storage, partition):
        """
        Return local low-water mark of a storage node. local low-water mark is
        the smallest valid transaction id of the partition in the storage.
        """
        self.logger.info("Retrieving local low-water mark of storage {}".format(storage))
        partition_info = self.storage_cli.list_partition(storage)
        regex = "Partition Info for id:\s*{}(.|\n)+localLowWaterMark:\s*(-?\d+)".format(partition)
        return int(search(regex, partition_info).group(2))

    def get_storage_session_id(self, storage, partition):
        """
        Return session id of a storage node.
        """
        self.logger.info("Retrieving session id of storage {}".format(storage))
        partition_info = self.storage_cli.list_partition(storage)
        regex = "Partition Info for id:\s*{}(.|\n)+SessionId:\s*(-?\d+)".format(partition)
        return int(search(regex, partition_info).group(2))

    def get_cluster_key(self):
        self.logger.info("Retrieving cluster key from zookeeper")
        zookeeper_metadata = self.zk_cli.list_zookeeper_metadata()
        return search("key\s*=\s*([^\s]+)", zookeeper_metadata).group(1)

    def get_server_node_idx(self, partition):
        """
        Return server node idx which the partition been assigned to.
        Server node index equals to server node id - 1.
        """
        self.logger.info("Retrieving server node that owns partition {}".format(partition))
        zookeeper_metadata = self.zk_cli.list_zookeeper_metadata()
        regex = "server=(\d)*,\s*partition={}".format(partition)
        return int(search(regex, zookeeper_metadata).group(1)) - 1

    def get_host(self, hostname, port):
        return "{}:{}".format(hostname, port)

    def set_storage_nodes_to_skip(self, nodes_idx):
        self.storage_nodes_to_skip = nodes_idx

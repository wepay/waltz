from waltz_ducktape.services.base_waltz_service import BaseWaltzService


class WaltzStorageService(BaseWaltzService):
    """
    WaltzStorageService is the service class for Waltz Storage.
    """
    def __init__(self, context, cluster_spec, zk, cluster_root, cluster_num_partitions, cluster_key, port, admin_port,
                 jetty_port, lib_dir, data_dir, config_file_dir):
        """
        Construct a new 'WaltzStorageService' object.

        :param context: The test context
        :param cluster_spec: The cluster specifics
        :param zk: Zookeeper url
        :param cluster_root: The cluster root
        :param cluster_num_partitions: Number of partitions in cluster
        :param cluster_key: The cluster key
        :param port: The service port
        :param admin_port: The admin port
        :param jetty_port: The jetty port
        :param lib_dir: The library directory
        :param data_dir: The data directory
        :param config_file_dir: The directory of configuration file path
        """
        super(WaltzStorageService, self).__init__(context, cluster_spec=cluster_spec, zk=zk, cluster_root=cluster_root, port=port, \
                                                  lib_dir=lib_dir, config_file_dir=config_file_dir)
        self.cluster_num_partitions = cluster_num_partitions
        self.cluster_key = cluster_key
        self.admin_port = admin_port
        self.jetty_port = jetty_port
        self.data_dir = data_dir

    def start_cmd(self):
        return "sudo systemctl start waltz-storage"

    def restart_cmd(self):
        return "sudo systemctl restart waltz-storage"

    def stop_cmd(self):
        return "sudo systemctl stop waltz-storage"

    def clean_cmd(self):
        return "sudo rm -rf {}".format(self.data_dir)

    def healthcheck_cmd(self, hostname):
        return "sudo curl -Is {}:{}/health | head -1".format(hostname, self.jetty_port)

    def provision_cmd(self):
        # make waltz the owner of write ahead log directory, since waltz
        # is the user to run /etc/systemd/system/waltz-storage.service
        return "sudo mkdir -p {} && sudo chown -R waltz {}".format(self.data_dir, self.data_dir)

    def render_log_file(self):
        return self.render('log4j.properties')

    def render_service_config_file(self):
        return self.render('waltz_storage.yaml', port=self.port, admin_port=self.admin_port, jetty_port=self.jetty_port,
                           data_directory=self.data_dir, cluster_num_partitions=self.cluster_num_partitions,
                           cluster_key=self.cluster_key)

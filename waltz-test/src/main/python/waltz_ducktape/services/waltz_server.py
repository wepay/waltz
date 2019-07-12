from waltz_ducktape.services.base_waltz_service import BaseWaltzService


class WaltzServerService(BaseWaltzService):
    """
    WaltzStorageService is the service class for Waltz Storage.
    """
    def __init__(self, context, cluster_spec, zk, cluster_root, cluster_name, cluster_num_partitions, port, jetty_port,
                 lib_dir, config_file_dir, ssl_configs):
        """
        Construct a new 'WaltzStorageService' object.

        :param context: The test context
        :param cluster_spec: The cluster specifics
        :param zk: Zookeeper url
        :param cluster_root: The cluster root
        :param cluster_name: The cluster name
        :param cluster_num_partitions: The number of partitions in cluster
        :param port: The service port
        :param jetty_port: The jetty port
        :param lib_dir: The library directory
        :param config_file_dir: The directory of configuration file
        :param ssl_configs: A dict of ssl related configurations
        """
        super(WaltzServerService, self).__init__(context, cluster_spec=cluster_spec, zk=zk, cluster_root=cluster_root, port=port, \
                                                 lib_dir=lib_dir, config_file_dir=config_file_dir)
        self.zk = zk
        self.cluster_root = cluster_root
        self.cluster_name = cluster_name
        self.cluster_num_partitions = cluster_num_partitions
        self.jetty_port = jetty_port
        self.ssl_configs = ssl_configs

    def start_cmd(self):
        return "sudo systemctl start waltz-server"

    def restart_cmd(self):
        return "sudo systemctl restart waltz-server"

    def stop_cmd(self):
        return "sudo systemctl stop waltz-server"

    def clean_cmd(self):
        return ""

    def healthcheck_cmd(self, hostname):
        return "sudo curl -Is {hostname}:{jetty_port}/health | head -1".format(hostname=hostname, jetty_port=self.jetty_port)

    def provision_cmd(self):
        return ""

    def render_log_file(self):
        return self.render('log4j.properties')

    def render_service_config_file(self):
        return self.render('waltz_server.yaml', cluster_root=self.cluster_root, \
            zk_connect=self.zk, port=self.port, jetty_port=self.jetty_port)

    def render_cli_config_file(self):
        return self.render('cli.yaml', cluster_root=self.cluster_root,
                           zk_connect=self.zk,
                           ssl_keystore_loc=self.ssl_configs["ssl_keystore_loc"],
                           ssl_keystore_pwd=self.ssl_configs["ssl_keystore_pwd"],
                           ssl_truststore_loc=self.ssl_configs["ssl_truststore_loc"],
                           ssl_truststore_pwd=self.ssl_configs["ssl_truststore_pwd"])


from waltz_ducktape.services.base_waltz_service import BaseWaltzService


class WaltzStorageService(BaseWaltzService):
    """
    WaltzStorageService is the service class for Waltz Storage.
    """
    def __init__(self, context, cluster_spec, zk, cluster_root, port, admin_port, jetty_port, lib_dir, data_dir,
                 config_file_dir, ssl_configs):
        """
        Construct a new 'WaltzStorageService' object.

        :param context: The test context
        :param cluster_spec: The cluster specifics
        :param zk: Zookeeper url
        :param cluster_root: The cluster root
        :param port: The service port
        :param admin_port: The admin port
        :param jetty_port: The jetty port
        :param lib_dir: The library directory
        :param data_dir: The data directory
        :param config_file_dir: The directory of configuration file path
        :param ssl_configs: A dict of ssl related configurations
        """
        super(WaltzStorageService, self).__init__(context, cluster_spec=cluster_spec, zk=zk, cluster_root=cluster_root, port=port, \
                                                  lib_dir=lib_dir, config_file_dir=config_file_dir)
        self.zk = zk
        self.cluster_root = cluster_root
        self.admin_port = admin_port
        self.jetty_port = jetty_port
        self.data_dir = data_dir
        self.ssl_configs = ssl_configs

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

    def service_config_file_path(self):
        return "{}/{}.yml".format(self.config_file_dir, "waltz_storage")

    def service_file_path(self):
        return "/etc/systemd/system/waltz-storage.service"

    def render_log_file(self):
        return self.render('log4j.properties')

    def render_service_config_file(self):
        return self.render('waltz_storage.yaml', port=self.port, admin_port=self.admin_port,
                           jetty_port=self.jetty_port, data_directory=self.data_dir, cluster_root=self.cluster_root,
                           zk_connect=self.zk, ssl_keystore_loc=self.ssl_configs["ssl_keystore_loc"],
                           ssl_keystore_pwd=self.ssl_configs["ssl_keystore_pwd"],
                           ssl_truststore_loc=self.ssl_configs["ssl_truststore_loc"],
                           ssl_truststore_pwd=self.ssl_configs["ssl_truststore_pwd"])

    def render_service_file(self):
        return self.render('waltz_storage.service')

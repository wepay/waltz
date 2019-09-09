from ducktape.services.service import Service
from ducktape.services.background_thread import BackgroundThreadService

class VerifiableClient(BackgroundThreadService):
    """
    VerifiableClient wraps ClientCli for use in system testing.
    It allows propagate exceptions thrown in background threads.
    """
    def __init__(self, context, cluster_spec, zk, cluster_root, lib_dir, config_file_dir, ssl_configs):
        """
        Construct a new 'VerifiableClient' object.

        :param context: The test context
        :param cluster_spec: The cluster specifics
        :param zk: Zookeeper url
        :param cluster_root: The cluster root
        :param lib_dir: The library directory
        :param config_file_dir: The directory of configuration file
        :param ssl_configs: A dict of ssl related configurations
        """
        super(VerifiableClient, self).__init__(context, cluster_spec=cluster_spec)
        self.zk = zk
        self.cluster_root = cluster_root
        self.lib_dir = lib_dir
        self.config_file_dir = config_file_dir
        self.ssl_configs = ssl_configs
        self.validation_cmd = None
        self.validation_outputs = {}

    def start(self, validation_cmd):
        self.validation_cmd = validation_cmd
        self.validation_outputs.clear()
        Service.start(self)

    def _worker(self, idx, node):
        self.chown_config_file_dir(node)
        self.load_client_config_file(self.config_file_path(), node)
        self.load_log_config_file(self.log_file_path(), node)

        self.logger.info("Running Verification")
        self.logger.debug("VerifiableClient {} command: {}".format(idx, self.validation_cmd))
        self.validation_outputs[idx] = node.account.ssh_output(self.validation_cmd)

    def task_complete(self):
        return len(self.validation_outputs) >= len(self.nodes)

    def get_validation_result(self):
        result = ""
        for node in self.nodes:
            node_idx = self.idx(node)
            node_output = self.validation_outputs.get(node_idx, None)
            result += "Validation result for client node {}: \n{}\n".format(node_idx, node_output)
        return result

    def stop_node(self, node):
        node.account.ssh_capture("pgrep -f waltz | xargs sudo kill -9")

    def clean_node(self, node):
        pass

    def chown_config_file_dir(self, node):
        node.account.ssh("sudo chown -R `whoami` {}".format(self.config_file_dir))

    def load_client_config_file(self, config_file_path, node):
        client_config_file = self.render_client_config_file()
        self.logger.info("Loading {}".format(config_file_path))
        self.logger.debug(client_config_file)

        # Override service config file
        node.account.create_file(config_file_path, client_config_file)

    def load_log_config_file(self, log_file_path, node):
        log_file = self.render('log4j.properties', log_level="ERROR")
        self.logger.info("Loading {}".format(log_file_path))
        self.logger.debug(log_file)

        # Override log config file
        node.account.create_file(log_file_path, log_file)

    def render_client_config_file(self):
        return self.render('waltz_client.yaml',
                           cluster_root=self.cluster_root,
                           zk_connect=self.zk,
                           ssl_keystore_loc=self.ssl_configs["ssl_keystore_loc"],
                           ssl_keystore_pwd=self.ssl_configs["ssl_keystore_pwd"],
                           ssl_truststore_loc=self.ssl_configs["ssl_truststore_loc"],
                           ssl_truststore_pwd=self.ssl_configs["ssl_truststore_pwd"])

    def config_file_path(self):
        return "{}/{}.yaml".format(self.config_file_dir, "waltz_client")

    def log_file_path(self):
        return "{}/waltz-log4j.cfg".format(self.config_file_dir)
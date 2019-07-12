import re
from ducktape.services.service import Service
from ducktape.utils.util import wait_until


class BaseWaltzService(Service):
    """
    BaseWaltzService is the base class for Waltz service.
    """
    def __init__(self, context, cluster_spec, zk, cluster_root, port, lib_dir, config_file_dir):
        """
        Construct a new 'BaseWaltzService' object.

        :param context: The test context
        :param cluster_spec: The cluster specifics
        :param zk: Zookeeper url
        :param cluster_root: The cluster root
        :param port: The service port
        :param lib_dir: The library directory
        :param config_file_dir: The directory of configuration file
        """
        self.zk = zk
        self.cluster_root = cluster_root
        self.port = port
        self.lib_dir = lib_dir
        self.config_file_dir = config_file_dir

        super(BaseWaltzService, self).__init__(context, cluster_spec=cluster_spec)

    def start_node(self, node):
        self.chown_config_file_dir(node)
        self.load_service_config_file(node)
        self.load_log_config_file(node)
        self.load_cli_config_file(node)

        # Run provision command.
        node.account.ssh(self.provision_cmd())

        # Run start command.
        node.account.ssh(self.start_cmd())
        wait_until(lambda: self.alive(node), timeout_sec=10,
                   err_msg="{} server node failed to start".format(self.service_name()))

    def stop_node(self, node):
        node.account.ssh(self.stop_cmd())
        wait_until(lambda: not self.alive(node), timeout_sec=10,
                   err_msg="Timed out waiting for {} node to stop.".format(self.service_name()))

    def restart_node(self, node):
        node.account.ssh(self.restart_cmd())
        wait_until(lambda: self.alive(node), timeout_sec=10, \
                   err_msg="{} server node failed to restart".format(self.service_name()))

    def clean_node(self, node):
        if self.alive(node):
            self.logger.warn("{} {} was still alive at cleanup time. Stopping node ..."
                             .format(self.service_name(), node.account))
            self.stop_node(node)
        node.account.ssh(self.clean_cmd(), allow_fail=False)

    def get_admin_node(self):
        # Return a random node with cli config file loaded.
        # This node can be used to execute cli command.
        node = self.nodes[0]
        self.chown_config_file_dir(node)
        self.load_cli_config_file(node)
        return node

    def chown_config_file_dir(self, node):
        # Suppose we want to put config file under /etc/waltz-*,
        # make self the directory owner
        node.account.ssh("sudo chown -R `whoami` {}".format(self.config_file_dir))

    def load_service_config_file(self, node):
        # Load config file.
        service_config_file = self.render_service_config_file()
        service_config_file_path = self.service_config_file_path()
        self.logger.info("Loading {}".format(service_config_file_path))
        self.logger.debug(service_config_file)

        # Override service config file
        node.account.create_file(service_config_file_path, service_config_file)

    def load_log_config_file(self, node):
        try:
            log_file = self.render_log_file()
            log_file_path = self.log_file_path()
            self.logger.info("Loading {}".format(log_file_path))
            self.logger.debug(log_file)

            # Override log config file
            node.account.create_file(log_file_path, log_file)
        except NotImplementedError:
            self.logger.info("Skipping loading log config file for {} server".format(self.service_name()))

    def load_cli_config_file(self, node):
        try:
            cli_config_file = self.render_cli_config_file()
            cli_config_file_path = self.cli_config_file_path()
            self.logger.info("Loading {}".format(cli_config_file_path))
            self.logger.debug(cli_config_file)

            # Override service config file
            node.account.create_file(cli_config_file_path, cli_config_file)
        except NotImplementedError:
            self.logger.info("Skipping loading cli config file or {} server".format(self.service_name()))

    def alive(self, node):
        cmd = self.healthcheck_cmd(node.account.hostname)
        health = node.account.ssh_output(cmd, allow_fail=False)
        if "HTTP/1.1 200 OK" in health:
            self.logger.debug("{} server started accepting connections at: {}:{})"
                              .format(self.service_name(), node.account.hostname, self.port))
            return True
        else:
            return False

    def service_name(self):
        camel_case_name = type(self).__name__
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camel_case_name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

    def service_config_file_path(self):
        return "{}/{}.yml".format(self.config_file_dir, "config")

    def log_file_path(self):
        return "{}/waltz-log4j.cfg".format(self.config_file_dir)

    def cli_config_file_path(self):
        return "{}/{}.yml".format(self.config_file_dir, "cli")

    def start_cmd(self):
        raise NotImplementedError

    def restart_cmd(self):
        raise NotImplementedError

    def stop_cmd(self):
        raise NotImplementedError

    def clean_cmd(self):
        raise NotImplementedError

    def healthcheck_cmd(self, hostname):
        raise NotImplementedError

    def provision_cmd(self):
        raise NotImplementedError

    def render_log_file(self):
        raise NotImplementedError

    def render_service_config_file(self):
        raise NotImplementedError

    def render_cli_config_file(self):
        raise NotImplementedError

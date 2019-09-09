class Cli(object):
    """
    Abstract class for cli utility classes under waltz_ducktape.services.cli package.
    """
    def __init__(self, cli_config_path):
        """
        Construct a new 'ClientCli' object.

        :param cli_config_path: The path to client cli config file
        """
        self.cli_config_path = cli_config_path

    def build_cmd(self, cmd_arr):
        return " ".join(filter(None, map(str, cmd_arr)))

    def java_cli_class_name(self):
        raise NotImplementedError

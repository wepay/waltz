import json
from configparser import ConfigParser
from ducktape.tests.test import Test
from ducktape.cluster.cluster_spec import ClusterSpec
from waltz_ducktape.services.waltz_storage import WaltzStorageService
from waltz_ducktape.services.waltz_server import WaltzServerService
from waltz_ducktape.services.verifiable_client import VerifiableClient


class WaltzTest(Test):
    """
    WaltzTest is the base class for all Waltz tests, that manages setting
    up Waltz server, Waltz storage and verifiable client.
    """
    def __init__(self, test_context):
        """
        Construct a new 'WaltzTest' object.

        :param test_context: The test context
        """
        super(WaltzTest, self).__init__(test_context=test_context)

        config = ConfigParser()
        config.read('waltz-test/src/main/python/waltz_ducktape/config.ini')
        self.zk_cfg = config['Zookeeper']
        self.storage_cfg = config['Waltz Storage']
        self.server_cfg = config['Waltz Server']
        self.client_cfg = config['Waltz Client']

    def get_storage_service(self, num_nodes=None):
        """
        Return a Waltz Storage service that uses config.ini for configuration.
        Optional arguments can be pass to override default settings.
        """
        num_nodes = num_nodes or int(self.storage_cfg['NumNodes'])
        cpu = int(self.storage_cfg['NumCpuCores'])
        mem = self.storage_cfg['MemSize']
        disk = self.storage_cfg['DiskSize']
        additional_disks = json.loads(self.storage_cfg['AdditionalDisks'])
        cluster_spec = ClusterSpec.from_dict({'cpu':cpu, 'mem':mem, 'disk':disk, 'num_nodes':num_nodes, 'additional_disks':additional_disks})
        zk = self.zk_cfg['ZkUrl']
        cluster_root = self.zk_cfg['ClusterRoot']
        port = int(self.storage_cfg['Port'])
        admin_port = int(self.storage_cfg['AdminPort'])
        jetty_port = int(self.storage_cfg['JettyPort'])
        lib_dir = self.storage_cfg['LibDir']
        data_dir = self.storage_cfg['DataDir']
        config_file_dir = self.storage_cfg['ConfigFileDir']
        ssl_configs = {"ssl_keystore_loc": self.storage_cfg['SslKeystoreLoc'],
                       "ssl_keystore_pwd": self.storage_cfg['SslKeystorePwd'],
                       "ssl_truststore_loc": self.storage_cfg['SslTruststoreLoc'],
                       "ssl_truststore_pwd": self.storage_cfg['SslTruststorePwd']}

        return WaltzStorageService(self.test_context, cluster_spec, zk, cluster_root, port, admin_port, jetty_port,
                                   lib_dir, data_dir, config_file_dir, ssl_configs)

    def get_server_service(self, cluster_num_partitions=None, num_nodes=None):
        """
        Return a Waltz Server service that uses config.ini for configuration.
        Optional arguments can be pass to override default settings.
        """
        cluster_num_partitions = cluster_num_partitions or int(self.zk_cfg['ClusterNumPartitions'])
        num_nodes = num_nodes or int(self.server_cfg['NumNodes'])
        cpu = int(self.server_cfg['NumCpuCores'])
        mem = self.server_cfg['MemSize']
        disk = self.server_cfg['DiskSize']
        cluster_spec = ClusterSpec.from_dict({'cpu':cpu, 'mem':mem, 'disk':disk, 'num_nodes':num_nodes})
        zk = self.zk_cfg['ZkUrl']
        cluster_root = self.zk_cfg['ClusterRoot']
        cluster_name = self.zk_cfg['ClusterName']
        port = int(self.server_cfg['Port'])
        jetty_port = int(self.server_cfg['JettyPort'])
        lib_dir = self.server_cfg['LibDir']
        config_file_dir = self.server_cfg['ConfigFileDir']
        ssl_configs = {"ssl_keystore_loc": self.server_cfg['SslKeystoreLoc'],
                       "ssl_keystore_pwd": self.server_cfg['SslKeystorePwd'],
                       "ssl_truststore_loc": self.server_cfg['SslTruststoreLoc'],
                       "ssl_truststore_pwd": self.server_cfg['SslTruststorePwd']}

        return WaltzServerService(self.test_context, cluster_spec, zk, cluster_root, cluster_name,
                                  cluster_num_partitions, port, jetty_port, lib_dir, config_file_dir, ssl_configs)

    def get_verifiable_client(self, num_nodes=None):
        """
        Return a verifiable Waltz client that uses config.ini for configuration.
        Optional arguments can be pass to override default settings.
        """
        num_nodes = num_nodes or int(self.client_cfg['NumNodes'])
        cpu = int(self.client_cfg['NumCpuCores'])
        mem = self.client_cfg['MemSize']
        disk = self.client_cfg['DiskSize']
        cluster_spec = ClusterSpec.from_dict({'cpu':cpu, 'mem':mem, 'disk':disk, 'num_nodes':num_nodes})
        zk = self.zk_cfg['ZkUrl']
        cluster_root = self.zk_cfg['ClusterRoot']
        lib_dir = self.client_cfg['LibDir']
        config_file_dir = self.client_cfg['ConfigFileDir']
        ssl_configs = {"ssl_keystore_loc": self.client_cfg['SslKeystoreLoc'],
                       "ssl_keystore_pwd": self.client_cfg['SslKeystorePwd'],
                       "ssl_truststore_loc": self.client_cfg['SslTruststoreLoc'],
                       "ssl_truststore_pwd": self.client_cfg['SslTruststorePwd']}
        return VerifiableClient(self.test_context, cluster_spec, zk, cluster_root, lib_dir, config_file_dir, ssl_configs)

    def str_to_bool(self, str):
        if str.lower() == 'true':
            return True
        elif str.lower() == 'false':
            return False
        else:
            raise ValueError("Cannot covert {} to a bool".format(str))

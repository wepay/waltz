---
id: waltz-setup
title: Waltz Setup
---

## Dependency

* ZooKeeper

## Things to decide before setup

* the cluster name
* the cluster root path (the path to the znode that represents the cluster)
* the number of partitions
* the number of replicas
* the placement of storage nodes (zone/region)
** the size of disks (persistent disks in which storage nodes store data)

## Creating a Waltz Cluster

There are 3 steps to setup a Waltz cluster.

(Note: The cli_config_path file should contain information about other config parameters such as "zookeeper.connectString", "cluster.root", "zookeeper.sessionTimeout" and ssl config prefix i.e. "ssl." (wherever required).

Step 1: Use `create` command to create a cluster

```
waltz_uber=/path/to/waltz-uber-X.Y.Z.jar
java ${waltz_uber+-cp ${waltz_uber}} \
    -Dwaltz.config=${cli_config_path?} \
    com.wepay.waltz.tools.zk.ZooKeeperCli \
    create \
    --name ${cluster_name?} \
    --partitions ${partition_count?}
```

Step 2: Use `add-storage-node` command to add storage node(s)

```
java ${waltz_uber+-cp ${waltz_uber}} \
    com.wepay.waltz.tools.zk.ZooKeeperCli \
    add-storage-node \
    --storage ${storage_node?} \
    --group ${group?} \
    --cli-config-path ${cli_config_path?}
```

where group is meant to represent a collection of storage nodes that should contain a complete replica of the data.  This is meant to map to a region; for example, group="us-central"

Step 3: Use `auto-assign` command to assign partitions to storage node(s) in group (with Round Robin)

```
java ${waltz_uber+-cp ${waltz_uber}} \
    com.wepay.waltz.tools.zk.ZooKeeperCli \
    auto-assign \
    --group ${group?} \
    --cli-config-path ${cli_config_path?}
```

At this point, start up only the storage nodes.

Step 4: Use `add-partition` command to assign partitions to storage node(s) based on what was assigned in ZooKeeper:

```
java ${waltz_uber+-cp ${waltz_uber}} \
    com.wepay.waltz.tools.storage.StorageCli \
    add-partition \
    --partition ${partition?} \
    --storage ${storage_node?} \
    --cli-config-path ${cli_config_path?}
```

The SSL config should be specified in `cli_config_path` file. The `storage` parameter should point to the storage node's admin port.

You will need to do this for every single replica assignment on every storage node. To get a list of replica assignments, you can run the list command to dump server metadata from ZooKeeper.

```
java ${waltz_uber+-cp ${waltz_uber}} \
    com.wepay.waltz.tools.zk.ZooKeeperCli \
    list \
    --cli-config-path ${cli_config_path?}
```

Once all replicas have been added to all storage nodes, you can start the Waltz servers. Running the list command again should show the servers now.

To manually assign a partition to storage node(s), you can use `assign-partition` command.

```
java ${waltz_uber+-cp ${waltz_uber}} \
    com.wepay.waltz.tools.zk.ZooKeeperCli \
    assign-partition \
    --partition ${partition?} \
    --storage ${storage_node?} \
    --cli-config-path ${cli_config_path?}
```

To manually un-assign a partition from storage node(s), you can use `unassign-partition` command.

```
java ${waltz_uber+-cp $waltz_uber} \
    com.wepay.waltz.tools.zk.ZooKeeperCli \
    unassign-partition \
    --partition ${partition?} \
    --storage ${storage_node?} \
    --cli-config-path ${cli_config_path?}
```

To remove a storage node from cluster, you can use `remove-storage-node` command.

```
java ${waltz_uber+-cp $waltz_uber} \
    com.wepay.waltz.tools.zk.ZooKeeperCli \
    remove-storage-node \
    --storage ${storage_node?} \
    --cli-config-path ${cli_config_path?}
```

To delete server metadata, you can use `delete` command.

```
java ${waltz_uber+-cp $waltz_uber} \
    com.wepay.waltz.tools.zk.ZooKeeperCli \
    delete \
    --name ${cluster_name?} \
    --force \ # force delete even if cluster name don't match
    --cli-config-path ${cli_config_path?}
```

## Configuration Parameters

Waltz uses YAML for configuration files. Configuration parameters have the form of dot-delimited names. When assigning a value "V" to a parameter "A.B.C", all of the following YAML is valid.

```
A.B.C: V
```

```
A.B:
  C: V
```

```
A:
  B.C: V
```

```
A:
  B:
    C: V
```

## Configuring WaltzServer

### Minimum Configuration Parameters

The following is the minimum required configuration parameters.


| Parameter Name           | Description                    | Default Value   |
| ----------------         | -------------                  | --------------- |
| zookeeper.connectString  | zookeeper connect string       |                 |
| zookeeper.sessionTimeout | zookeeper session timeout      |                 |
| cluster.root             | cluster root path in Zookeeper |                 |
| server.port              | server port number             |                 |

### Parameters for Metrics

This setting enables the following endpoints.

* /ping
* /buildinfo
* /metrics
* /health

| Parameter Name    | Description              | Default Value   |
| ----------------  | -------------            | --------------- |
| server.jetty.port | server jetty port number |                 |

### Security Configuration Parameters

It is strongly recommended to set security configuration parameters in production.

#### Configuration parameters for the client-server communication

| Parameter Name                    | Description                                        | Default Value   |
| ----------------                  | -------------                                      | --------------- |
| server.ssl.keyStore.location      | key store location path                            |                 |
| server.ssl.keyStore.password      | key store password                                 |                 |
| server.ssl.keyStore.type          | key store type                                     | JKS             |
| server.ssl.trustStore.location    | trust store location path                          |                 |
| server.ssl.trustStore.password    | trust store password                               |                 |
| server.ssl.trustStore.type        | trust store type                                   | JKS             |
| server.ssl.keyManager.algorithm   | algorithm used when creating a KeyManagerFactory   | SunX509         |
| server.ssl.trustManager.algorithm | algorithm used when creating a TrustManagerFactory | SunX509         |

#### Configuration parameters for the server-storage communication

| Parameter Name                     | Description                                        | Default Value   |
| ----------------                   | -------------                                      | --------------- |
| storage.ssl.keyStore.location      | key store location path                            |                 |
| storage.ssl.keyStore.password      | key store password                                 |                 |
| storage.ssl.keyStore.type          | key store type                                     | JKS             |
| storage.ssl.trustStore.location    | trust store location path                          |                 |
| storage.ssl.trustStore.password    | trust store password                               |                 |
| storage.ssl.trustStore.type        | trust store type                                   | JKS             |
| storage.ssl.keyManager.algorithm   | algorithm used when creating a KeyManagerFactory   | SunX509         |
| storage.ssl.trustManager.algorithm | algorithm used when creating a TrustManagerFactory | SunX509         |

### More Server Configuration Parameters

See _com.wepay.waltz.server.WaltzServerConfig_ for more configuration parameters.

## Starting a WaltzServer instance

```
java -cp waltz-uber-X.Y.Z.jar com.wepay.waltz.server.WaltzServer <config file path>
```

## Configuring Waltz Storage Server

Create a property file as a configuration file.

### Minimum Configuration Parameters

The following is the minimum required configuration parameters.

| Parameter Name        | Description                                                                                                | Default Value    |
| ----------------      | -------------                                                                                              | ---------------  |
| storage.port          | storage port number                                                                                        |                  |
| storage.admin         | port	storage admin port number                                                                          |                    |
| storage.directory     | path to data directory                                                                                     |                  |
| storage.segment       | size.threshold	threshold of segment size (a new segment is created when a segment size exceed this value) | 1000000000 |
| cluster.key           | cluster key (which is a unique identifier for the cluster)                                                 |                  |
| cluster.numPartitions | total number of partitions in the cluster                                                                  |                  |

### Parameters for Metrics

This setting enables the following endpoints.

* /ping
* /buildinfo
* /metrics
* /health

| Parameter Name     | Description               | Default Value   |
| ----------------   | -------------             | --------------- |
| storage.jetty.port | storage jetty port number |                 |

## Security Configuration Parameters

It is strongly recommended to set security configuration parameters in production.

### Configuration parameters for the server-storage communication

| Parameter Name                     | Description                                        | Default Value   |
| ----------------                   | -------------                                      | --------------- |
| storage.ssl.keyStore.location      | key store location path
| storage.ssl.keyStore.password      | key store password
| storage.ssl.keyStore.type          | key store type                                     | JKS             |
| storage.ssl.trustStore.location    | trust store location path                          |                 |
| storage.ssl.trustStore.password    | trust store password                               |                 |
| storage.ssl.trustStore.type        | trust store type                                   | JKS             |
| storage.ssl.keyManager.algorithm   | algorithm used when creating a KeyManagerFactory   | SunX509         |
| storage.ssl.trustManager.algorithm | algorithm used when creating a TrustManagerFactory | SunX509         |

## Starting a WaltzStorage instance

```
java -cp waltz-uber-X.Y.Z.jar com.wepay.waltz.storage.WaltzStorage <config file path>
```

## Configuring WaltzClient

Typically Waltz client is configured programatically using java.util.Properties and com.wepay.waltz.client.WaltzClientConfig. WaltzClientConfig defines symbols for configuration parameters.

```
Map<Object, Object> config = new HashMap<>();
config.put(WaltzClientConfig.ZOOKEEPER_CONNECT_STRING, "zkserve1:2181,zkserver2:2181,zkserver3:2181");
config.put(WaltzClientConfig.ZOOKEEPER_SESSION_TIMEOUT, "30000");
config.put(WaltzClientConfig.CLUSTER_ROOT, "/test/cluster");
...
WaltzClient client = new WaltzClient(callback, new WaltzClientConfig(config));
```

### Minimum Configuration Parameters

The following is the minimum required configuration parameters.

| Parameter Name           | Description                    | Default Value   |
| ----------------         | -------------                  | --------------- |
| zookeeper.connectString  | zookeeper connect string       |                 |
| zookeeper.sessionTimeout | zookeeper session timeout      |                 |
| cluster.root             | cluster root path in Zookeeper |                 |

### Security Configuration Parameters

It is strongly recommended to set security configuration parameters in production.

#### Configuration parameters for the client-server communication

| Parameter Name                    | Description                                        | Default Value   |
| ----------------                  | -------------                                      | --------------- |
| client.ssl.keyStore.location      | key store location path                            |                 |
| client.ssl.keyStore.password      | key store password                                 |                 |
| client.ssl.keyStore.type          | key store type                                     | JKS             |
| client.ssl.trustStore.location    | trust store location path                          |                 |
| client.ssl.trustStore.password    | trust store password                               |                 |
| client.ssl.trustStore.type        | trust store type                                   | JKS             |
| client.ssl.keyManager.algorithm   | algorithm used when creating a KeyManagerFactory   | SunX509         |
| client.ssl.trustManager.algorithm | algorithm used when creating a TrustManagerFactory | SunX509         |

### Other Configuration Parameters

| Parameter Name                    | Description                                        | Default Value   |
| ----------------                  | -------------                                      | --------------- |
| client.numTransactionRetryThreads | number of threads used for retries of transactions | 1               |

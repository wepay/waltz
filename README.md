# Waltz

Waltz is a distributed/replicated write ahead log for transactions.

## Building

To build Waltz:

    ./gradlew clean build

## Testing

To run a single test class:

    ./gradlew clean :waltz-storage:test -Dtest.single=StorageClientTest

To run a single test method:

    ./gradlew clean :waltz-storage:test --tests com.wepay.waltz.storage.client.StorageClientTest.testBasicReadWrite

### Smoke test

Waltz also comes with a smoke test that starts:

* 1 ZooKeeper server
* 2 Waltz clients
* 3 Waltz servers
* 3 Waltz storage nodes

It then sends 1 million transactions while turning the servers on and off at random. Afterwards, it validates that all transactions were received, and that the checksums of the data are identical. It also logs throughput and latency.

To run smoke tests:

    bin/smoketest.sh

The smoke test output looks like:

    0001[... .*. *]
    0002[... .** *]
    0003[... *** *]
    0004[..* *** *]
    0005[*.* *** *]
    0006[*** *** *] --..-..-..-..-+..-..-+..-..-..-..-+..-..-..-..-..-..-+..+
    0007[.** *** *] +++-..-..-..-
    0008[*** *** *] +..-..-
    0009[*** .** *]
    0010[*.* .** *] +++..-..-..-..-..-..+-
    0011[*.* *** *] ..
    0012[*** *** *] +-..-..-..-+..-+..-..-..-..+-..-

Each state change results in a new line. The first four digits indicate the number of server/storage start/stops that have been triggered so far. The `[... ... .]` portion indicates the state of the server nodes, storage nodes and the zookeeper server, where `*` means that the process is up, and `.` means that it's down. Lastly, the trailing part of the line indicates 1000 writes (`-`), 1000 reads (`.`), and retries (`+`).

A log file (`smoketest.log`) can be found in your home directory.

## Demo app

Waltz comes with a demo app that shows an example account balance database built on top of Waltz. 

Start a test Waltz cluster in docker environment.

    bin/test-cluster.sh start

Start a MySQL instance in docker environment.

    bin/demo-bank-db.sh start

Run demo application with simple self-explanatory commands.

    bin/demo-bank-app.sh

## Run Waltz in Docker

We implemented shell scripts to run a Waltz cluster in local Docker containers for testing purpose.

### Creating the Docker images

    ./gradlew distDocker

This builds the Docker images.

### Starting the test cluster

    bin/test-cluster.sh start

This creates a user defined docker network `waltz-network` and 
starts three container, a zookeeper server, a waltz storage node, and a waltz server node in `waltz-network`.

Zookeeper port is 2181 inside `waltz-network` and exposed to the host machine at 42181.
So, if you want to run a Waltz application outside of `waltz-network`, use `yourHostName:42181` for `zookeeper.connectString`.
The cluster's root ZNode is `/waltz_cluster`. So specify this as `cluster.root`.

If the Docker images are not built yet, this script builds them.
However, it doesn't automatically build a new images even when the source code is modified. 
You must rebuild images using `distDocker` gradle task.

### Stopping the test cluster

    bin/test-cluster.sh stop

This stops all three containers. You can resume the cluster using `start` command. All data in zookeeper and storages are preserved.

### Destroying the cluster

    bin/test-cluster.sh clean
    
This will remove all three containers, thus removes all data.

### Running DemoBankApp

First create a database. The following command will create a mysql container and the database.

    bin/demo-bank-db.sh start

Then, start the demo application:

    bin/demo-bank-app.sh
    
To stop the MySQL instance:

    bin/demo-bank-db.sh stop

To remove the database:

    bin/demo-bank-app.sh clean

## Publishing Waltz Docs

Go through website/README.md

## Waltz performance and integration tests

This package contains Python-based Waltz integration and performance tests. Waltz uses [ducktape](https://ducktape-docs.readthedocs.io) as its integration test framework, and Vagrant as its test environment.

### Setup

Set up a virtual environment, and activate it.

```
virtualenv <some path>/waltz-test
source <some path>/waltz-test/bin/activate
```

Install [ducktape](https://ducktape-docs.readthedocs.io).

```
pip install ducktape
```

(If you have problems, see the [ducktape installation](https://ducktape-docs.readthedocs.io/en/latest/install.html) page.)

#### Install Vagrant

From the Waltz root, run:

```
vagrant status
```

If you don't have Vagrant or vagrant-google plugin installed, then install them:

Install [Vagrant](https://www.vagrantup.com/downloads.html).

Install vagrant-google plugin:

```
vagrant plugin install vagrant-google
```
Install inifile plugin:

```
vagrant plugin install inifile
```

Install Vagrant hostmanager:

```
vagrant plugin install vagrant-hostmanager
```

#### Prepare keystore and truststore

Put `keystore.jks` and `truststore.jks` under `waltz/vagrant/sync/server` and `waltz/vagrant/sync/storage`'
```
waltz
└─waltz/vagrant
  └─waltz/vagrant/sync
    ├─waltz/vagrant/sync/server
    │ ├─waltz/vagrant/sync/server/keystore.jks
    │ └─waltz/vagrant/sync/server/truststore.jks
    └─waltz/vagrant/sync/storage
      ├─waltz/vagrant/sync/storage/keystore.jks
      └─waltz/vagrant/sync/storage/truststore.jks

```
#### Set configuration file

To create VM instance in `poc`, fill in the missing fields from `waltz-test/src/main/python/waltz_ducktape/config.ini`.
```
GoogleJsonKeyLocation=
WaltzSshUsername=
WaltzSshPrivateKeyPath=
```
To create VM instance in other environment, some other fields need to be reset.

#### Start Vagrant

From the Waltz root, run:

```
vagrant up --provider=google --no-parallel
```

#### Running the tests

Run all the tests:

```
ducktape waltz-test/src/main/python
```

The results will be stored in a `results` directory in the Waltz repository root.

#### Check VM service log (optional)
To ssh to GCE virtual machine:
```
ssh [gce-vm-instance-name]
```
To view service log:
```
journalctl -u [waltz-storage | waltz-server] -e -f
```
or
```
systemctl status [waltz-storage | waltz-server]
```

#### Destroy Vagrant

From the Waltz root, run:

```
vagrant destroy
```

## What's going on here?

There are two componenets to the integration tests:

* Vagrant
* Ducktape

Vagrant is used to manage the virtual machines that are used for testing. We use four nodes: one ZooKeeper node, and three Waltz nodes. Vagrant has pluggable "providers" that provide virtual machines, and we use [GCP provider](https://github.com/mitchellh/vagrant-google).

Ducktape is [Confluent's](https://www.confluent.io/) integration test framework for [Apache Kafka](https://github.com/apache/kafka/tree/trunk/tests). It is responsible for starting up services in the virtual machines provided by Vagrant and google/gce, and for running tests on the services once they're started. It also collects logs from the remote virtual machines, and stores them locally in a "results" directory locally.

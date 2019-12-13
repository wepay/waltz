## Waltz Integration & Performance Tests

This package contains Python-based Waltz integration & performance tests. Waltz uses [ducktape](https://ducktape-docs.readthedocs.io) as its integration test framework, and Vagrant as its test environment.

### Run Waltz Tests

#### Set up Virtual Environment

```
virtualenv <destination_directory>=
source <destination_directory>/bin/activate
```

#### Install [ducktape](https://github.com/wepay/ducktape)

```
git clone git@github.com:wepay/ducktape.git
python setup.py install
```

#### Install [Vagrant](https://www.vagrantup.com/docs/)

First, install vagrant from [here](https://www.vagrantup.com/downloads.html)

Then, install vagrant plugins and hostmanager

```
vagrant plugin install vagrant-google
vagrant plugin install inifile
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

Fill in missing fields from `waltz-test/src/main/python/waltz_ducktape/config.ini`.

```
# Waltz common GCE configuration (Please fill in all fields)
# see guidance at https://github.com/mitchellh/vagrant-google#google-cloud-platform-setup
GoogleProjectId=
GoogleClientEmail=
GoogleJsonKeyLocation=
GoogleNetwork=
GoogleSubnetwork=

# GCE SSH configuration (Please fill in all fields)
WaltzSshUsername=
WaltzSshPrivateKeyPath=

# Waltz storage GCE configuration (Please fill in all fields)
WaltzStorageImage=
WaltzStorageBootDiskSizeGb=
# e.g. [{"image_family": [STRING], "image_project_id": [STRING], "disk_size": [NUMBERS_OF_GB]}]
WaltzStorageAdditionalDisks=

# Waltz server GCE configuration (Please fill in all fields)
WaltzServerImage=
WaltzServerBootDiskSizeGb=

# Waltz client GCE configuration (Please fill in all fields)
WaltzClientImage=
WaltzClientBootDiskSizeGb=
```

#### Start Vagrant

Start and provision Vagrant environment

```
cd <path_to_waltz_directory>
vagrant up --provider=google --no-parallel
```

Check if all Vagrant machines are up
```
vagrant status
```

#### Run integration test

Ducktape discovers and runs tests in the path provided, here are some ways to run tests:

```
ducktape <relative_path_to_testdirectory>               # e.g. ducktape dir/tests
ducktape <relative_path_to_file>                        # e.g. ducktape dir/tests/my_test.py
ducktape <path_to_test>[::SomeTestClass]                # e.g. ducktape dir/tests/my_test.py::TestA
ducktape <path_to_test>[::SomeTestClass[.test_method]]  # e.g. ducktape dir/tests/my_test.py::TestA.test_a
```

The results will be stored in a `results` directory in the Waltz repository root.

#### Check VM service log (optional)
SSH to GCE virtual machine, e.g server-0, storage-1
```
ssh <gce_vm_instance_name_defined_in_vagrantfile]
```
View service log
```
journalctl -u [waltz-storage | waltz-server] -e -f
systemctl status [waltz-storage | waltz-server]
```

#### Destroy Vagrant

Stops and deletes all traces of the vagrant machine

```
vagrant destroy
```

## What's going on here?

There are two components to the integration tests:

* Vagrant
* Ducktape

Vagrant is used to manage the virtual machines that are used for testing. We use four nodes: one ZooKeeper node, and three Waltz nodes. Vagrant has pluggable "providers" that provide virtual machines, and we use [GCP provider](https://github.com/mitchellh/vagrant-google).

Ducktape is [Confluent's](https://www.confluent.io/) integration test framework for [Apache Kafka](https://github.com/apache/kafka/tree/trunk/tests). It is responsible for starting up services in the virtual machines provided by Vagrant and google/gce, and for running tests on the services once they're started. It also collects logs from the remote virtual machines, and stores them locally in a "results" directory locally.

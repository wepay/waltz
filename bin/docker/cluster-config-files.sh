#!/bin/sh

cluster_name="/waltz_cluster_$1"
server_base_port=$2
server_jetty_port=$((server_base_port + 2))
storage_base_port=$3
storage_admin_port=$((storage_base_port + 1))
storage_jetty_port=$((storage_base_port + 2))
storage_directory="/waltz_storage/"

mkdir -p build/config-$1

sed  \
    "s|\(cluster.root:[ ]*\)\(.*$\)|\1$cluster_name|;" \
    config/local-docker/waltz-tools.yml > build/config-$1/waltz-tools.yml

sed \
    "s|\(storage.port:[ ]*\)\([0-9]*\)|\1$storage_base_port|;
    s|\(admin.port:[ ]*\)\([0-9]*\)|\1$storage_admin_port|;
    s|\(jetty.port:[ ]*\)\([0-9]*\)|\1$storage_jetty_port|;
    s|\(storage.directory:[ ]*\)\(.*$\)|\1$storage_directory|;
    s|\(cluster.root:[ ]*\)\(.*$\)|\1$cluster_name|;" \
    config/local-docker/waltz-storage.yml > build/config-$1/waltz-storage.yml

sed \
    "s|\(server.port:[ ]*\)\([0-9]*\)|\1$server_base_port|;
    s|\(jetty.port:[ ]*\)\([0-9]*\)|\1$server_jetty_port|;
    s|\(cluster.root:[ ]*\)\(.*$\)|\1$cluster_name|;" \
    config/local-docker/waltz-server.yml > build/config-$1/waltz-server.yml

echo "config files are generated in build/config-$1"

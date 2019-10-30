#!/bin/sh

DIR=$(dirname $0)
cmd=$1

imageSource=docker
tag=5.7
imageName=mysql:$tag
containerName=mysql-demo

networkName=waltz-network
ports=43306:3306

mysqlRootPass=pass1234
schemaName=DEMO_BANK
dbUser=demo
dbUserPass=demo

runContainer() {
    docker run \
	    -p "$ports" \
	    --name "$containerName" \
	    -e MYSQL_ROOT_PASSWORD="$mysqlRootPass" \
	    -d "$imageName" \
    || die
    newContainerStarted=true
}

source $DIR/docker/container.sh

if [ "$newContainerStarted" == true ]
then
    echo "...creating database"
    sleep 10
    docker exec -i $containerName sh <<EOF
until mysql -uroot -ppass1234 -e "select 'MySQL started';";
do
    echo "...waiting for MySQL to start";
    sleep 1;
done
EOF

    docker exec -i $containerName mysql -uroot -p${mysqlRootPass} << EOF
create database if not exists ${schemaName};
grant all on ${schemaName}.* to "${dbUser}"@localhost identified by "$dbUserPass";
grant all on ${schemaName}.* to "${dbUser}"@"%" identified by "$dbUserPass";
EOF

    echo "...database created"

    mkdir -p build/config

    sed -e "s/<<host>>/$(hostname)/" config/local-docker/demo-app.yml > build/config/demo-app.yml
fi

#!/bin/sh

DIR=$(dirname $0)
cmd=$1

imageSource=waltz-server:distDocker
imageName=com.wepay.waltz/waltz-server
containerName="$2_server"
networkName=waltz-network
configFolder="$2"

if [ $cmd = "start" ]; then
    portsLowerBound=$3
    portsUpperBound=$(($3 + 2))
    ports="$portsLowerBound-$portsUpperBound:$portsLowerBound-$portsUpperBound"
fi

runContainer() {
    local imageId=$(docker images -q ${imageName})
    if [ "${imageId}" = "" ]
    then
        echo "...image not built correctly"
    else
        docker run \
            --network=$networkName --net-alias $(hostname) -h $(hostname) -p $ports \
            --name $containerName -d -v $PWD/config/local-docker/$configFolder:/config/ \
            $imageId /config/waltz-server.yml
    fi
}

source $DIR/container.sh

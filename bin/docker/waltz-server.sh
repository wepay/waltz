#!/bin/sh

DIR=$(dirname $0)
cmd=$1

imageSource=waltz-server:distDocker
imageName=com.wepay.waltz/waltz-server
containerName=waltz-server-"$2"
networkName=waltz-network
configFolder="config-$2"

if [ $cmd = "start" ]; then
    port_lower_bound=$3
    port_upper_bound=$4
    ports="$port_lower_bound-$port_upper_bound:$port_lower_bound-$port_upper_bound"
fi

runContainer() {
    local imageId=$(docker images -q ${imageName})
    if [ "${imageId}" == "" ]
    then
        echo "...image not built correctly"
    else
        docker run \
            --network=$networkName --net-alias $(hostname) -h $(hostname) -p $ports \
            --name $containerName -d -v $PWD/build/$configFolder:/config/ \
            $imageId /config/waltz-server.yml
    fi
}

source $DIR/container.sh

findImage() {
    # check if the image is there
    local imageId=$(docker images -q ${imageName})
    if [ "${imageId}" == "" ]
    then
        if [ $imageSource == "docker" ]
        then
            echo "...image not found, pulling an image"
            docker pull $imageName
            imageId=$(docker images -q ${imageName})
            echo "...image pulled [$imageName]"
        else
            echo "...image not found, building an image"
            $DIR/../../gradlew $imageSource
            echo "...image built [$imageName]"
        fi
    fi
}

startContainer() {
    # check if the container is running
    local containerId=$(docker ps -q -f name=${containerName})
    if [ "${containerId}" != "" ]
    then
        echo "...container already running [$containerName]"
    else
        # check if the container is stopped
        containerId=$(docker ps -q -a -f name=${containerName})
        if [ "${containerId}" != "" ]
        then
            docker start $containerName
            echo "...container resumed [$containerName]"
        else
            runContainer
            echo "...container started [$containerName]"
        fi
    fi
}

stopContainer() {
    # check if the container is running
    local containerId=$(docker ps -q -f name=${containerName})
    if [ "${containerId}" != "" ]
    then
        docker stop $containerName
        echo "...container stopped [$containerName]"
    else
        echo "...container already stopped [$containerName]"
    fi
}

removeContainer() {
    # check if the container is stopped
    local containerId=$(docker ps -q -f name=${containerName})
    if [ "${containerId}" != "" ]
    then
        echo "...container still running [$containerName], not removed"
    else
        containerId=$(docker ps -q -a -f name=${containerName})
        if [ "${containerId}" != "" ]
        then
            docker rm $containerName
            echo "...container removed [$containerName]"
        else
            echo "...container not found [$containerName]"
        fi
    fi
}

case $cmd in
    start)
        echo "---- starting $containerName -----"
        findImage
        startContainer
        ;;

    stop)
        echo "---- stopping $containerName -----"
        stopContainer
        ;;

    clean)
        echo "---- removing $containerName -----"
        removeContainer
        ;;
    *)
        echo "invalid command [$cmd]"
        ;;
esac

die() { test -n "$*" && echo "$*"; exit 1; } >&2

findImage() {
    # pull or build an image, as necessary
    local imageId
    imageId=$(docker images -q ${imageName}) || die
    if test -z "${imageId}"; then
        if [ $imageSource == "docker" ]
        then
            echo "$imageName not found, pulling..."
            docker pull $imageName || die
        else
            echo "$imageName not found, building..."
            $DIR/../../gradlew $imageSource || die
        fi
    fi
}

startContainer() {
    # start a container if not already running
    local containerId
    containerId=$(docker ps -q -f name=${containerName}) || die
    if test -n "${containerId}"; then
        echo "$containerName container already running [$containerId]"
    else
        # check if the container is stopped
        containerId=$(docker ps -q -a -f name=${containerName}) || die
        if test -n "${containerId}"; then
            echo "starting container $containerName [$containerId]"
            docker start $containerName || die
        else
            runContainer || die
        fi
    fi
}

stopContainer() {
    # stop container, if running
    local containerId
    containerId=$(docker ps -q -f name=${containerName}) || die
    if test -n "${containerId}"; then
        echo "stopping $containerName [$containerId]"
        docker stop $containerId || die
    else
        echo "$containerName not running"
    fi
}

removeContainer() {
    # remove container if not running
    local containerId
    containerId=$(docker ps -q -f name=${containerName}) || die
    if test -n "${containerId}"; then
        echo "$containerName still running [$containerId], not removed"
    else
        containerId=$(docker ps -q -a -f name=${containerName}) || die
        if test -n "${containerId}"; then
            echo "Removing container $containerName [$containerId]"
            docker rm "$containerName" || die
        else
            echo "container $containerName not found"
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
        die "invalid command [$cmd]"
        ;;
esac

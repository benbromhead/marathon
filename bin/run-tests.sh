#!/bin/bash

#
# Run all tests including integration tests via docker.
#
# The script places some files into target/docker-build-volumes.
# If you keep it around between builds your can greatly speed up resolving artifacts
# but on the other hand you do not test if all artifacts are still resolvable.
#

# be verbose and print all commands, better for debugging
set -x

PROJECT_DIR=`dirname $0`/..
PROJECT_DIR=$(cd "$PROJECT_DIR"; pwd)

if [ -z "$1" ]; then
    echo "Usage:" >&2
    echo "$0 <unique build id without crazy characters or spaces>" >&2
    exit 1
fi

function fatal {
    echo "$1" >&2
    exit 2
}

BUILD_ID=$1

# We preserve some files across builds if the target directory is not deleted in between.
# Especially the ivy cache can speed up builds substantially.
BUILD_VOLUME_DIR="$PROJECT_DIR/docker-volumes"
mkdir -p "$BUILD_VOLUME_DIR" || fatal "Couldn't created '$BUILD_VOLUME_DIR'" >&2
rm -rf "$BUILD_VOLUME_DIR/targets" || fatal "Couldn't clean '$BUILD_VOLUME_DIR/targets'" >&2

# Cleanup left-over containers
function cleanup {
    echo Cleaning up volumes
    docker rmi marathon-buildbase:$BUILD_ID 2>/dev/null
    docker rm -v marathon-itests-$BUILD_ID 2>/dev/null
}

cleanup
trap cleanup EXIT

if ! docker build -t marathon-buildbase:$BUILD_ID -f "$PROJECT_DIR/Dockerfile.build-base" "$PROJECT_DIR"; then
    fatal "Build for the buildbase failed" >&2
fi

docker run \
    --rm=true \
    --name marathon-itests-$BUILD_ID \
    -v "$BUILD_VOLUME_DIR/sbt:/root/.sbt" \
    -v "$BUILD_VOLUME_DIR/ivy2:/root/.ivy2" \
    -v "$BUILD_VOLUME_DIR/targets/main:/marathon/target" \
    -v "$BUILD_VOLUME_DIR/targets/project:/marathon/project/target" \
    -v "$BUILD_VOLUME_DIR/targets/mesos-simulation:/marathon/mesos-simulation/target" \
    -i \
    marathon-buildbase:$BUILD_ID \
    sbt -Dsbt.log.format=false \
    test \
    integration:test \
    "project mesosSimulation" integration:test "test:runMain mesosphere.mesos.scale.DisplayAppScalingResults"

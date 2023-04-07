#!/usr/bin/env bash

CHRONOS_VERSION=1.0

echo "Chronos Pinned Build"

if [ $# -eq 0 ] || [ -z "$1" ]
   then
      echo "Usage:"
      echo "./chronos_pinned_build.sh DEPS_DIR BUILD_DIR JOBS"
      echo "  DEPS_DIR: directory where to place build dependencies (same deps as for Mandel 3.1)"
      echo "  BUILD_DIR: build directory"
      echo "  JOBS: number of parallel processes"      
      exit -1
fi

DEP_DIR=$1
BUILD_DIR=$2
JOBS=$3


SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";

MORE_CMAKE_FLAGS="-DCHRONICLE_RECEIVER_NAME=chronos-writer -DCHRONICLE_RECEIVER_ADDITIONAL_PLUGINS=${SCRIPT_DIR}/writer -DCHRONICLE_JSON_EXPORT=FALSE -DPACKAGE_NAME=chronos -DPACKAGE_VERSION=${CHRONOS_VERSION}" ${SCRIPT_DIR}/external/chronicle/pinned_build/chronicle_pinned_build.sh ${DEP_DIR} ${BUILD_DIR} ${JOBS}


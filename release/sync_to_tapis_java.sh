#!/bin/sh
# Sync tapis-systemsapi, tapis-systemslib directories to tapis-java repo.
set -xv
DST=../../tapis-java
API_SRC=../tapis-systemsapi
LIB_SRC=../tapis-systemslib

# Determine absolute path to location from which we are running
#  and change to that directory.
export RUN_DIR=$(pwd)
export PRG_RELPATH=$(dirname "$0")
cd "$PRG_RELPATH"/. || exit
export PRG_PATH=$(pwd)

# Make sure service has been built
if [ ! -d "$DST" ]; then
  echo "tapis-java directory not found at: $DST"
  exit 1
fi

# Sync directories using rsync
rsync -rv --delete $API_SRC $DST
rsync -rv --delete $LIB_SRC $DST

# Fix up pom files using gres_r
cd $DST/tapis-systemsapi
gres_r "<artifactId>tapis-systems</artifactId>" "<artifactId>tapis</artifactId>" pom.xml
cd $DST/tapis-systemslib
gres_r "<artifactId>tapis-systems</artifactId>" "<artifactId>tapis</artifactId>" pom.xml

cd $RUN_DIR

#!/bin/sh
# Run docker image for tapis/systems-migratejob using env variables set locally.
# Tag for image must in as first argument
# Following env variables must be set:
#   TAPIS_SITE_ID
#   TAPIS_SERVICE_PASSWORD
#   TAPIS_TENANT_SVC_BASEURL
#   TAPIS_DB_JDBC_URL
#   TAPIS_DB_USER
#   TAPIS_DB_PASSWORD
# Following services from a running tapis3 are required: tenants, tokens, security-kernel

PrgName=$(basename "$0")

USAGE1="Usage: $PrgName <image_version_tag>"
USAGE2="For example $PrgName 1.1.5"

TAG="tapis/systems-migratejob:$1"

##########################################################
# Check number of arguments.
##########################################################
if [ $# -ne 1 -o -z "$1" ]; then
  echo "Please provide image tag"
  echo $USAGE1
  echo $USAGE2
  exit 1
fi

if [ -z "$TAPIS_SITE_ID" -o -z "$TAPIS_SERVICE_PASSWORD" -o -z "$TAPIS_TENANT_SVC_BASEURL" \
      -o -z "$TAPIS_DB_JDBC_URL" -o -z "$TAPIS_DB_USER" -o -z "$TAPIS_DB_PASSWORD" ]; then
  echo "Please set env variables:"
  echo "  TAPIS_SITE_ID, TAPIS_SERVICE_PASSWORD and TAPIS_TENANT_SVC_BASEURL"
  echo "  TAPIS_DB_JDBC_URL, TAPIS_DB_USER, TAPIS_DB_PASSWORD"
  echo $USAGE1
  echo $USAGE2
  exit 1
fi

# Determine absolute path to location from which we are running.
export RUN_DIR=$(pwd)
export PRG_RELPATH=$(dirname "$0")
cd "$PRG_RELPATH"/. || exit
export PRG_PATH=$(pwd)

# Running with network=host exposes ports directly. Only works for linux
docker run --rm -i --network="host"  \
           -e TAPIS_SERVICE_PASSWORD="${TAPIS_SERVICE_PASSWORD}" \
           -e TAPIS_TENANT_SVC_BASEURL="$TAPIS_TENANT_SVC_BASEURL" \
           -e TAPIS_SITE_ID="$TAPIS_SITE_ID" \
           -e TAPIS_DB_JDBC_URL="$TAPIS_TAPIS_DB_JDBC_URL" \
           -e TAPIS_DB_USER="$TAPIS_TAPIS_DB_USER" \
           -e TAPIS_DB_PASSWORD="$TAPIS_TAPIS_DB_PASSWORD" \
           "${TAG}"
cd "$RUN_DIR"

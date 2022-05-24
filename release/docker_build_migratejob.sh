#!/bin/sh
# Build and optionally push docker image for Systems service migrate job
# This is the job run in Jenkins as an optional part of job TapisJava->3_ManualBuildDeploy->systems
# Existing docker login is used for push
# Docker image is created with a unique tag: tapis/<SVC_NAME>-<VER>-<COMMIT>-<YYYYmmddHHMM>
#   - other tags are created and updated as appropriate
#
PrgName=$(basename "$0")

USAGE="Usage: $PrgName [ -push ]"

SVC_NAME="systems-migratejob"
REPO="tapis"

BUILD_DIR=../tapis-systemsapi/target

# Check number of arguments and 1st arg if present
if [ $# -gt 1 ]; then
  echo "$USAGE"
  exit 1
elif [ $# -eq 1 ] && [ "x$1" != "x-push" ]; then
  echo "$USAGE"
  exit 1
fi

# Determine absolute path to location from which we are running
#  and change to that directory.
export RUN_DIR=$(pwd)
export PRG_RELPATH=$(dirname "$0")
cd "$PRG_RELPATH"/. || exit
export PRG_PATH=$(pwd)

# Make sure service has been built
if [ ! -d "$BUILD_DIR" ]; then
  echo "Build directory missing. Please build. Directory: $BUILD_DIR"
  exit 1
fi

# Copy Dockerfile to build dir
cp Dockerfile_migratejob $BUILD_DIR

# Move to the build directory
cd $BUILD_DIR || exit
# Set variables used for build
VER=$(cat classes/tapis.version)
GIT_BRANCH_LBL=$(awk '{print $1}' classes/git.info)
GIT_COMMIT_LBL=$(awk '{print $2}' classes/git.info)
TAG_UNIQ="${REPO}/${SVC_NAME}:${VER}-$(date +%Y%m%d%H%M)-${GIT_COMMIT_LBL}"
TAG_VER="${REPO}/${SVC_NAME}:${VER}"

# If branch name is UNKNOWN or empty as might be the case in a jenkins job then
#   set it to GIT_BRANCH. Jenkins jobs should have this set in the env.
if [ -z "$GIT_BRANCH_LBL" -o "x$GIT_BRANCH_LBL" = "xUNKNOWN" ]; then
  GIT_BRANCH_LBL=$(echo "$GIT_BRANCH" | awk -F"/" '{print $2}')
fi

# Build image from Dockerfile
echo "Building local image using primary tag: $TAG_UNIQ"
echo "  VER=        ${VER}"
echo "  GIT_BRANCH_LBL= ${GIT_BRANCH_LBL}"
echo "  GIT_COMMIT_LBL= ${GIT_COMMIT_LBL}"
docker build -f Dockerfile_migratejob \
   --label VER="${VER}" --label GIT_COMMIT="${GIT_COMMIT_LBL}" --label GIT_BRANCH="${GIT_BRANCH_LBL}" \
    -t "${TAG_UNIQ}" .

# Create other tags
echo "Creating image for local testing user tag: $TAG_VER"
docker tag "$TAG_UNIQ" "$TAG_VER"

# Push to remote repo
if [ "x$1" = "x-push" ]; then
  echo "Pushing images to docker hub."
  # NOTE: Use current login. Jenkins job does login
  docker push "$TAG_UNIQ"
  docker push "$TAG_VER"
fi
cd "$RUN_DIR"

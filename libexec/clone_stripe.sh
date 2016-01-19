#!/bin/sh

source /grid/fermiapp/products/common/etc/setups.sh
#---------------
setup fife_utils $FIFE_UTILS_VERSION
#for now, while testing
#cd /nova/app/users/mengel/fife_utils
#setup -. fife_utils
#setup awscli
#export IFDH_DEBUG=1
#printenv
#set -x
#---------------------
# aws credentials...
touch ${TMPDIR:-}/.awst
chmod 600 ${TMPDIR:-}/.awst
ifdh cp /pnfs/${EXPERIMENT}/scratch/users/${GRID_USER:-$USER}/awst ${TMPDIR:-/var/tmp}/.awst
source ${TMPDIR:-}/.awst
#---------------

sam_clone_dataset --experiment=${SAM_EXPERIMENT} "$@"

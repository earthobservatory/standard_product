#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# source ISCE env
export STANDARD_PRODUCT_HOME=$HOME/standard_product
export PYTHONPATH=$BASE_PATH:$PYTHONPATH
export PATH=$BASE_PATH:$PATH

# source environment
source $HOME/verdi/bin/activate

echo "##########################################" 1>&2
echo -n "Running S1 create interferogram sciflo: " 1>&2
date 1>&2
/usr/bin/python $BASE_PATH/sciflo_stage_iw_slc.py > sciflo_stage_iw_slc.log 2>&1
STATUS=$?
echo -n "Finished running stage iw slc sciflo: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run stage iw slc sciflo." 1>&2
  cat sciflo_stage_iw_slc.log 1>&2
  echo "{}"
  exit $STATUS
fi

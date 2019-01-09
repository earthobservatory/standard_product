#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# source PGE env
export PYTHONPATH=$BASE_PATH:$PYTHONPATH
export PYTHONPATH=${PYTHONPATH}:${HOME}/verdi/etc
export PATH=$BASE_PATH:$PATH

# source environment
source $HOME/verdi/bin/activate


echo "##########################################" 1>&2
echo -n "Running initiate_standard_product_localizer: " 1>&2
date 1>&2
/usr/bin/python $BASE_PATH/initiate_standard_product_localizer.py > initiate_standard_product_localizer.log 2>&1
STATUS=$?
echo -n "Finished running initiate_standard_product_localizer: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run initiate_standard_product_localizer." 1>&2
  cat initiate_standard_product_localizer.log 1>&2
  echo "{}"
  exit $STATUS
fi

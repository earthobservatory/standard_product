#!/bin/bash

# clone spyddder-man to be moved to its final location by docker builder
git clone https://github.com/hysds/spyddder-man.git

# clone ariamh to be moved to its final location by docker builder
git clone -b standard-product https://github.com/mkarim2017/ariamh.git

#clone lightweight_water_mask
git clone https://github.jpl.nasa.gov/aria-hysds/lightweight_water_mask.git

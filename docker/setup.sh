#!/bin/bash

# clone spyddder-man to be moved to its final location by docker builder
git clone -b eos-opds https://github.com/earthobservatory/multi_acquisition_localizer.git

# clone ariamh to be moved to its final location by docker builder
git clone -b python3 https://github.com/hysds/ariamh.git


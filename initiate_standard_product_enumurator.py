#!/usr/bin/env python3

import os, sys, time, json, requests, logging
import orbit_acquisition_selector
import enumerate_acquisition



def main():
    context_file = os.path.abspath("_context.json")
    if not os.path.exists(context_file):
        raise(RuntimeError("Context file doesn't exist."))
    acq_array = orbit_acquisition_selector.resolve_aoi_acqs(context_file)
    enumerate_acquisition.enumerate_acquisations_array(acq_array)

if __name__ == "__main__":
    sys.exit(main())

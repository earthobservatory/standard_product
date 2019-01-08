#!/usr/bin/env python3

import os, sys, time, json, requests, logging
import orbit_acquisition_selector
import standard_product_localizer

def main():

    
    context_file = os.path.abspath("_context.json")
    if not os.path.exists(context_file):
        raise(RuntimeError("Context file doesn't exist."))
    
    orbit_acq_selections = orbit_acquisition_selector.resolve_aoi_acqs(context_file)
    standard_product_localizer.resolve_source(context_file)

if __name__ == "__main__":
    sys.exit(main())

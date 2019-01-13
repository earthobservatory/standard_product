#!/usr/bin/env python

import os, sys, time, json, requests, logging
import standard_product_localizer

def main():

    
    context_file = os.path.abspath("_context.json")
    if not os.path.exists(context_file):
        raise(RuntimeError("Context file doesn't exist."))
    try:   
        standard_product_localizer.resolve_source(context_file)
    except Exception as e:
        raise

if __name__ == "__main__":
    sys.exit(main())

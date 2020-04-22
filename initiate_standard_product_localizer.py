#!/usr/bin/env python

import os, sys, time, json, requests, logging, traceback
import standard_product_localizer

def main():

    
    context_file = os.path.abspath("_context.json")
    if not os.path.exists(context_file):
        raise(RuntimeError("Context file doesn't exist."))
    try:   
        standard_product_localizer.resolve_source(context_file)
    except Exception as e:
        raise


if __name__ == '__main__':
    try: status = main()
    except (Exception, SystemExit) as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
    sys.exit(status)

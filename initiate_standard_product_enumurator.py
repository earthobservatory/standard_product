#!/usr/bin/env python3

import os, sys, time, json, requests, logging
import orbit_acquisition_selector
import standard_product_enumurator
import publish_initiator



def main():
    context_file = os.path.abspath("_context.json")
    if not os.path.exists(context_file):
        raise(RuntimeError("Context file doesn't exist."))
    orbit_acq_selections = orbit_acquisition_selector.resolve_aoi_acqs(context_file)
    #print("\n\norbit_acq_selections:\n%s" %orbit_acq_selections)
    candidate_pair_list = standard_product_enumurator.enumerate_acquisations(orbit_acq_selections)
    for candidate_pair in candidate_pair_list:
        print("\n\nMaster ACQS : ")
        for acq in  candidate_pair["master_acqs"]:
            print(acq)

        print("\n\nSLAVE ACQS : ")
        for acq in  candidate_pair["slave_acqs"]:
            print(acq)
    publish_initiator.publish_initiator(candidate_pair_list, orbit_acq_selections["job_data"])

if __name__ == "__main__":
    sys.exit(main())

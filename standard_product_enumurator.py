import os, sys, re, requests, json, logging, traceback, argparse, copy, bisect
import util
#from hysds.celery import app
import os, sys, re, requests, json, logging, traceback, argparse, copy, bisect
import hashlib
from itertools import product, chain
from datetime import datetime, timedelta
import numpy as np
from osgeo import ogr, osr
from pprint import pformat
from collections import OrderedDict
from shapely.geometry import Polygon
from util import ACQ

#import isce
#from UrlUtils import UrlUtils as UU


# set logger and custom filter to handle being run from sciflo
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger('enumerate_acquisations')
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())

RESORB_RE = re.compile(r'_RESORB_')

SLC_RE = re.compile(r'(?P<mission>S1\w)_IW_SLC__.*?' +
                    r'_(?P<start_year>\d{4})(?P<start_month>\d{2})(?P<start_day>\d{2})' +
                    r'T(?P<start_hour>\d{2})(?P<start_min>\d{2})(?P<start_sec>\d{2})' +
                    r'_(?P<end_year>\d{4})(?P<end_month>\d{2})(?P<end_day>\d{2})' +
                    r'T(?P<end_hour>\d{2})(?P<end_min>\d{2})(?P<end_sec>\d{2})_.*$')

IFG_ID_TMPL = "S1-IFG_R{}_M{:d}S{:d}_TN{:03d}_{:%Y%m%dT%H%M%S}-{:%Y%m%dT%H%M%S}_s{}-{}-{}"
RSP_ID_TMPL = "S1-SLCP_R{}_M{:d}S{:d}_TN{:03d}_{:%Y%m%dT%H%M%S}-{:%Y%m%dT%H%M%S}_s{}-{}-{}"

BASE_PATH = os.path.dirname(__file__)
GRQ_ES_URL = "http://100.64.134.208:9200/"
covth = 0.98
MIN_MAX = 2


def get_overlapping_slaves_query(master):
    query = {
            "query": {
                "filtered": {
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "term": {
                                        "dataset.raw": "acquisition-S1-IW_SLC"
                                    }
				
                                }
                            ]
                        }
                    },
                    "filter": {
 			"bool": {
			    "must": [
				{
                                "geo_shape": {
                                    "location": {
                                      "shape": master.location
                                    }
                                }},
				{	
                                "range" : {
                                    "endtime" : {
                                        "lte" : master.starttime
                
                                    }
                                }},
				{ "term": { "trackNumber": master.tracknumber }},
				{ "term": { "direction": master.direction }}
			    ],
			"must_not": { "term": { "orbitNumber": master.orbitnumber }}
			}
                    }
                }
            },
            "partial_fields" : {
                "partial" : {
                        "exclude": "city"
                }
            }
        }    

    return query

def get_overlapping_masters_query(master, slave):
    query = {
            "query": {
                "filtered": {
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "term": {
                                        "dataset.raw": "acquisition-S1-IW_SLC"
                                    }
				
                                }
                            ]
                        }
                    },
                    "filter": {
 			"bool": {
			    "must": [
				{
                                "geo_shape": {
                                    "location": {
                                      "shape": master.location
                                    }
                                }},
				{ "term": { "direction": master.direction }},
	                        { "term": { "orbitNumber": master.orbitnumber }},
			        { "term": { "trackNumber": master.tracknumber }}
			    ]
			}
                    }
                }
            },
            "partial_fields" : {
                "partial" : {
                        "exclude": "city"
                }
            }
        }    

    return query

def process_query(query):

    rest_url = GRQ_ES_URL
    #dav_url =  "https://aria-dav.jpl.nasa.gov"
    #version = "v1.1"
    grq_index_prefix = "grq"

    logger.info("query: {}".format(json.dumps(query, indent=2)))

    if rest_url.endswith('/'):
        rest_url = rest_url[:-1]

    # get index name and url
    url = "{}/{}/_search?search_type=scan&scroll=60&size=100".format(rest_url, grq_index_prefix)
    logger.info("url: {}".format(url))
    r = requests.post(url, data=json.dumps(query))
    r.raise_for_status()
    scan_result = r.json()
    count = scan_result['hits']['total']
    print("count : %s" %count)
    scroll_id = scan_result['_scroll_id']
    ref_hits = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' % rest_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        ref_hits.extend(res['hits']['hits'])

    return ref_hits



def enumerate_acquisations(orbit_acq_selections):
    job_data = orbit_acq_selections["job_data"]
    orbit_aoi_data = orbit_acq_selections["orbit_aoi_data"]

    orbit_file = job_data['orbit_file']

    for aoi_id in orbit_aoi_data.keys():
        aoi_data = orbit_aoi_data[aoi_id]
        selected_track_acqs = aoi_data['selected_track_acqs'] 
        #logger.info("%s : %s\n" %(aoi_id, selected_track_acqs))

        for track in selected_track_acqs.keys():
            for orbitnumber in selected_track_acqs[track].keys():
          
                slaves_track = {}
                slave_acqs = []
                logger.info("Enumeration %s : %s\n" %(aoi_id, track))
            
                selected_acqs = selected_track_acqs[track][orbitnumber]
                master_track_ipf_count = util.get_ipf_count(selected_acqs)

                util.print_acquisitions(aoi_id, selected_acqs)

                for acq in selected_acqs:
                    logger.info("\nMASTER ACQ : %s\t%s\t%s\t%s\t%s\t%s\t%s" %(acq.tracknumber, acq.starttime, acq.endtime, acq.pv, acq.direction, acq.orbitnumber, acq.identifier))
                    ref_hits = []
                    query = get_overlapping_slaves_query(acq)
                    matched_acqs = util.create_acqs_from_metadata(process_query(query))
                    logger.info("\nSLAVE ACQS")
                    util.print_acquisitions(aoi_id, matched_acqs)
     
                    slave_acqs.extend(matched_acqs)
                    #logger.info(matched_acqs)
                
                grouped_matched = util.group_acqs_by_orbit_number(slave_acqs)
                #matched_ids = grouped_matched["acq_info"].keys()
                #logger.info(grouped_matched["acq_info"].keys())

                orbitnumber_pv = {}
                logger.info("\n\n\nTRACK : %s" %track)
                for orbitnumber in sorted( grouped_matched["grouped"][track], reverse=True):
                    pv_list = []
                    #orbit_count= orbit_count+1
                    logger.info("SortedOrbitNumber : %s" %orbitnumber)
                    for pv in grouped_matched["grouped"][track][orbitnumber]:
                       logger.info("\tpv : %s" %pv)
                       pv_list.append(pv)
                    orbitnumber_pv[orbitnumber] = len(list(set(pv_list)))
                

                if master_track_ipf_count == 1:
                    for orbitnumber in sorted( grouped_matched["grouped"][track], reverse=True):
                        slave_ipf_count = orbitnumber_pv[orbitnumber]
                                    
                elif master_track_ipf_count > 1:
                    for orbitnumber in sorted( grouped_matched["grouped"][track], reverse=True):
                        slave_ipf_count = orbitnumber_pv[orbitnumber]




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
es_index = "grq_*_*acquisition*"



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

    candidate_pair_list = []

    for aoi_id in orbit_aoi_data.keys():
        aoi_data = orbit_aoi_data[aoi_id]
        selected_track_acqs = aoi_data['selected_track_acqs'] 
        #logger.info("%s : %s\n" %(aoi_id, selected_track_acqs))

        for track in selected_track_acqs.keys():
            for orbitnumber in selected_track_acqs[track].keys():
                slaves_track = {}
                slave_acqs = []
                logger.info("Enumeration %s : %s\n" %(aoi_id, track))
            
                master_acqs = selected_track_acqs[track][orbitnumber]
                master_track_ipf_count = util.get_ipf_count(master_acqs)

                util.print_acquisitions(aoi_id, master_acqs)

                for acq in master_acqs:
                    logger.info("\nMASTER ACQ : %s\t%s\t%s\t%s\t%s\t%s\t%s" %(acq.tracknumber, acq.starttime, acq.endtime, acq.pv, acq.direction, acq.orbitnumber, acq.identifier))
                    ref_hits = []
                    query = util.get_overlapping_slaves_query(acq)
       
                    acqs = [i['fields']['partial'][0] for i in util.query_es2(query, es_index)]
                    logger.info("Found {} slave acqs : {}".format(len(acqs),
                    json.dumps([i['id'] for i in acqs], indent=2)))


                    #matched_acqs = util.create_acqs_from_metadata(process_query(query))
                    matched_acqs = util.create_acqs_from_metadata(acqs)
                    logger.info("\nSLAVE ACQS")
                    util.print_acquisitions(aoi_id, matched_acqs)
     
                    slave_acqs.extend(matched_acqs)
                    #logger.info(matched_acqs)
                
                slave_grouped_matched = util.group_acqs_by_orbit_number(slave_acqs)
                #matched_ids = grouped_matched["acq_info"].keys()
                #logger.info(grouped_matched["acq_info"].keys())
                 
                orbitnumber_pv = {}
                logger.info("\n\n\nTRACK : %s" %track)
                slave_acqs_orbitnumber = []
                for slave_orbitnumber in sorted( slave_grouped_matched["grouped"][track], reverse=True):
                    selected = util.water_mask_test(slave_grouped_matched["acq_info"], slave_grouped_matched["grouped"][track][slave_orbitnumber],  aoi_data['aoi_location'], orbit_file)
                    if not selected:
                        continue
                    pv_list = []
                    #orbit_count= orbit_count+1
                    #logger.info("SortedOrbitNumber : %s" %orbitnumber)
                    for pv in slave_grouped_matched["grouped"][track][slave_orbitnumber]:
                       logger.info("\tpv : %s" %pv)
                       pv_list.append(pv)
                       slave_ids= slave_grouped_matched["grouped"][track][slave_orbitnumber][pv]
                       for slave_id in slave_ids:
                           slave_acqs_orbitnumber.append(slave_grouped_matched["acq_info"][slave_id])
                    orbitnumber_pv[slave_orbitnumber] = len(list(set(pv_list)))
                
                if master_track_ipf_count == 1:
                    for slave_orbitnumber in sorted( slave_grouped_matched["grouped"][track], reverse=True):
                        slave_ipf_count = orbitnumber_pv[slave_orbitnumber]
                        if slave_ipf_count == 1:
                            for acq in master_acqs:
                                query = util.get_overlapping_slaves_query_orbit(acq, slave_orbitnumber)
                                slave_acqs = [i['fields']['partial'][0] for i in util.query_es2(query, es_index)]
                                logger.info("Found {} slave acqs : {}".format(len(slave_acqs),
                                json.dumps([i['id'] for i in slave_acqs], indent=2)))
                                if len(slave_acqs)>0:
                                    try:
                                        matched_slave_acqs = util.create_acqs_from_metadata(slave_acqs)

                                        matched, candidate_pair = check_match(acq, matched_slave_acqs, "master")
                                        if matched and len(candidate_pair)>0:
                                            candidate_pair_list.append(candidate_pair)

                                    except Exception as err:
                                        logger.info(str(err))
                                        traceback.print_exc()

                        elif slave_ipf_count> 1:
                            for acq in slave_acqs_orbitnumber:
                                matched = check_match(acq, master_acqs, "slave")
                                if matched:
                                    candidate_pair_list.append(candidate_pair)
                                '''
                                query = util.get_overlapping_slaves_query_orbit(acq)
                                query = util.get_overlapping_masters_query(master, acq):
                                slave_acqs = [i['fields']['partial'][0] for i in util.query_es2(query, es_index)]
                                logger.info("Found {} slave acqs : {}".format(len(acqs),
                                json.dumps([i['id'] for i in acqs], indent=2)))
                                matched_slave_acqs = util.create_acqs_from_metadata(slave_acqs)
                                '''


                elif master_track_ipf_count > 1:
                    for slave_orbitnumber in sorted( slave_grouped_matched["grouped"][track], reverse=True):
                        slave_ipf_count = orbitnumber_pv[slave_orbitnumber]
                        if slave_ipf_count == 1:
                            for acq in master_acqs:
                                query = util.get_overlapping_slaves_query_orbit(acq)
                                slave_acqs = [i['fields']['partial'][0] for i in util.query_es2(query, es_index)]
                                logger.info("Found {} slave acqs : {}".format(len(slave_acqs),
                                json.dumps([i['id'] for i in slave_acqs], indent=2)))
                                if len(slave_acqs)>0:
                                    try:
                                        matched_slave_acqs = util.create_acqs_from_metadata(slave_acqs)

                                        matched, candidate_pair = check_match(acq, matched_slave_acqs, "master")
                                        if matched and len(candidate_pair)>0:
                                            candidate_pair_list.append(candidate_pair)
                                    
                                    except Exception as err:
                                        logger.info(str(err))
                                        traceback.print_exc()
    return candidate_pair_list

   
def get_union_geometry(acq_dict):
    """Return polygon of union of acquisition footprints."""

    # geometries are in lat/lon projection
    #src_srs = osr.SpatialReference()
    #src_srs.SetWellKnownGeogCS("WGS84")
    #src_srs.ImportFromEPSG(4326)

    # get union geometry of all scenes
    geoms = []
    union = None
    #logger.info(acq_dict)
    ids = sorted(acq_dict.keys())
    for id in ids:
        geom = ogr.CreateGeometryFromJson(json.dumps(acq_dict[id]))
        geoms.append(geom)
        union = geom if union is None else union.Union(geom)
    union_geojson =  json.loads(union.ExportToJson())
    return union_geojson

def check_match(ref_acq, matched_acqs, ref_type = "master"):
    matched = False
    candidate_pair = {}
    overlapped_matches = util.find_overlap_match(ref_acq, matched_acqs)
    if len(overlapped_matches)>0:
        logger.info("Overlapped Acq exists")
        #logger.info("Overlapped Acq exists for track: %s orbit_number: %s process version: %s. Now checking coverage." %(track, orbitnumber, pv))
        union_loc = get_union_geometry(overlapped_matches)
        logger.info("union loc : %s" %union_loc)
        is_ref_truncated = util.ref_truncated(ref_acq, overlapped_matches, covth=.99)
        is_covered = util.is_within(ref_acq.location["coordinates"], union_loc["coordinates"])
        is_overlapped, overlap = util.is_overlap(ref_acq.location["coordinates"], union_loc["coordinates"])
        logger.info("is_ref_truncated : %s" %is_ref_truncated)
        logger.info("is_within : %s" %is_covered)
        logger.info("is_overlapped : %s, overlap : %s" %(is_overlapped, overlap))
        matched_acqs=[]
        for acq_id in overlapped_matches.keys():
            matched_acqs.append(acq_id[0])
        if is_overlapped and overlap>=0.98: # and overlap >=covth:
            logger.info("MATCHED")
            matched = True
            if ref_type == "master":
                candidate_pair = {"master_acqs" : [ref_acq.acq_id[0]], "slave_acqs" : matched_acqs}
            else:
                candidate_pair = {"master_acqs" : matched_acqs, "slave_acqs" : [ref_acq.acq_id[0]]}
        return matched, candidate_pair
            


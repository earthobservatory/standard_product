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
import gtUtil

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


    logger.info("\n\n\nENUMERATE\n")
    job_data = orbit_acq_selections["job_data"]
    orbit_aoi_data = orbit_acq_selections["orbit_aoi_data"]

    reject_pairs = {}
    orbit_file = job_data['orbit_file']

    candidate_pair_list = []

    for aoi_id in orbit_aoi_data.keys():
        aoi_data = orbit_aoi_data[aoi_id]
        selected_track_acqs = aoi_data['selected_track_acqs'] 
        #logger.info("%s : %s\n" %(aoi_id, selected_track_acqs))

        for track in selected_track_acqs.keys():
            min_max_count, track_candidate_pair_list = get_candidate_pair_list(selected_track_acqs[track], reject_pairs)
           
            if min_max_count >= MIN_MAX and len(track_candidate_pair_list) > 0:
                candidate_pair_list.extend(track_candidate_pair_list)
            
    return candidate_pair_list

def reject_list_check(candidate_pair, reject_list):
    passed = False
    if not reject_list:
        passed = True
    else:
        ''' IMPLEMENT LOGIC HERE '''
        passed = True
    return passed

def get_candidate_pair_list2(selected_track_acqs, reject_pairs):
    logger.info("get_candidate_pair_list : %s Orbits" %len(selected_track_acqs.keys()))
    candidate_pair_list = []
    orbit_ipf_dict = {}
    min_max_count = 0
    
    for orbitnumber in selected_track_acqs.keys():
        orbit_ipf_dict[orbitnumber] = util.get_ipf_count(selected_track_acqs[orbitnumber])

    number_of_orbits = len(orbit_ipf_dict.keys())
    for orbitnumber in sorted(selected_track_acqs.keys(), reverse=True):
        logger.info(orbitnumber)

    if number_of_orbits <=1:
        logger.info("Returning as number of orbit is : %s" %number_of_orbits)
        return 0, candidate_pair_list
    elif number_of_orbits == 2:
        master_orbit_number = sorted(orbit_ipf_dict.keys(), reverse=True)[0]
        logger.info("master_orbit_number : %s" %master_orbit_number)
        slave_orbit_number = sorted(orbit_ipf_dict.keys(), reverse=True)[1]
        logger.info("slave_orbit_number : %s" %slave_orbit_number)
        master_ipf_count = orbit_ipf_dict[master_orbit_number]
        master_acqs = selected_track_acqs[master_orbit_number]
        slave_ipf_count = orbit_ipf_dict[slave_orbit_number]
        slave_acqs = selected_track_acqs[slave_orbit_number]
        result, orbit_candidate_pair_list = process_enumeration(master_acqs, master_ipf_count, slave_acqs, slave_ipf_count, reject_pairs)
        if result and len(orbit_candidate_pair_list)>0:
            candidate_pair_list.extend(orbit_candidate_pair_list)
            min_max_count = min_max_count + 1
            if min_max_count>=MIN_MAX:
                return min_max_count, candidate_pair_list
        
    elif number_of_orbits > 2:

        for i in range(number_of_orbits-2):
            logger.info("\n\nProcessing : %s" %i)
            logger.info("OrbitNumbers : %s" %orbit_ipf_dict.keys())
            master_orbit_number = sorted(orbit_ipf_dict.keys(), reverse=True)[i]
            logger.info("master_orbit_number : %s" %master_orbit_number) 
            master_acqs = selected_track_acqs[master_orbit_number]
            master_ipf_count = orbit_ipf_dict[master_orbit_number]
            j = i+1

            while j<number_of_orbits:
                slave_orbit_number = sorted(orbit_ipf_dict.keys(), reverse=True)[j]
                logger.info("slave_orbit_number : %s" %slave_orbit_number)
                slave_ipf_count = orbit_ipf_dict[slave_orbit_number]
                slave_acqs = selected_track_acqs[slave_orbit_number]
                result, orbit_candidate_pair_list = process_enumeration(master_acqs, master_ipf_count, slave_acqs, slave_ipf_count, reject_pairs)
                if result and len(orbit_candidate_pair_list)>0:
                    candidate_pair_list.extend(orbit_candidate_pair_list)
                    min_max_count = min_max_count + 1
                    if min_max_count>=MIN_MAX:
                        return min_max_count, candidate_pair_list
    return min_max_count, candidate_pair_list
    
def process_enumeration(master_acqs, master_ipf_count, slave_acqs, slave_ipf_count, reject_pairs):
    result = False
    candidate_pair_list = []
    
    ref_type = None

    if slave_ipf_count == 1:
        for acq in master_acqs:
            result, candidate_pair = check_match(acq, slave_acqs, "master") 
            if not result:
                return False, []
            elif reject_list_check(candidate_pair, reject_pairs):
                candidate_pair_list.append(candidate_pair)
    elif slave_ipf_count > 1 and master_ipf_count == 1:
        for acq in slave_acqs:
            result, candidate_pair = check_match(acq, master_acqs, "slave")         
            if not result:
                return False, []
            elif reject_list_check(candidate_pair, reject_pairs):
                candidate_pair_list.append(candidate_pair)
    
    if len(candidate_pair_list) == 0:
        result = False
    return result, candidate_pair


def enumerate_acquisations(orbit_acq_selections):


    logger.info("\n\n\nENUMERATE\n")
    job_data = orbit_acq_selections["job_data"]
    orbit_aoi_data = orbit_acq_selections["orbit_aoi_data"]

    reject_pairs = {}
    orbit_file = job_data['orbit_file']

    candidate_pair_list = []

    for aoi_id in orbit_aoi_data.keys():
        logger.info("\n\nProcessing : %s " %aoi_id)
        aoi_data = orbit_aoi_data[aoi_id]
        selected_track_acqs = aoi_data['selected_track_acqs']
        #logger.info("%s : %s\n" %(aoi_id, selected_track_acqs))

        for track in selected_track_acqs.keys():
            min_max_count, track_candidate_pair_list = get_candidate_pair_list(track, selected_track_acqs[track], aoi_data, reject_pairs)
            logger.info("\n\nAOI ID : %s MIN MAX count for track : %s = %s" %(aoi_id, track, min_max_count))
            if min_max_count>0:
                print_candidate_pair_list_per_track(track_candidate_pair_list)
            if min_max_count >= MIN_MAX and len(track_candidate_pair_list) > 0:
                candidate_pair_list.extend(track_candidate_pair_list)

    return candidate_pair_list

def print_candidate_pair_list_per_track(candidate_pair_list):
    if len(candidate_pair_list)>0:
        for i in range(len(candidate_pair_list)):
            candidate_pair = candidate_pair_list[i]
            logger.info("Masters Acqs:")
            logger.info("%s : %s " %(type(candidate_pair), candidate_pair))
            #logger.info("Masters Acqs:")
            #logger.info(candidate_pair["master_acqs"])

            '''
            for j in range(len(candidate_pair["master_acqs"])):
                logger.info(candidate_pair["master_acqs"][j])
            for j in range(len(candidate_pair["slave_acqs"])):
                logger.info(candidate_pair["slave_acqs"][j])
            '''

def get_candidate_pair_list(track, selected_track_acqs, aoi_data, reject_pairs):
    logger.info("get_candidate_pair_list : %s Orbits" %len(selected_track_acqs.keys()))
    candidate_pair_list = []
    orbit_ipf_dict = {}
    min_max_count = 0
    aoi_location = aoi_data['aoi_location']
    logger.info("aoi_location : %s " %aoi_location)

    for orbitnumber in sorted(selected_track_acqs.keys(), reverse=True):
        logger.info(orbitnumber)
   
        slaves_track = {}
        slave_acqs = []
            
        master_acqs = selected_track_acqs[orbitnumber]
        master_ipf_count, master_starttime, master_endtime, master_location, master_track, direction, master_orbitnumber = util.get_union_data_from_acqs(master_acqs)
        #master_ipf_count = util.get_ipf_count(master_acqs)
        #master_union_geojson = util.get_union_geojson_acqs(master_acqs)

        #util.print_acquisitions(aoi_data['aoi_id'], master_acqs)
        query = util.get_overlapping_slaves_query(master_starttime, master_endtime, master_location, track, direction, master_orbitnumber)

        acqs = [i['fields']['partial'][0] for i in util.query_es2(query, es_index)]
        logger.info("Found {} slave acqs : {}".format(len(acqs),
        json.dumps([i['id'] for i in acqs], indent=2)))


                    #matched_acqs = util.create_acqs_from_metadata(process_query(query))
        slave_acqs = util.create_acqs_from_metadata(acqs)
        logger.info("\nSLAVE ACQS")
        #util.print_acquisitions(aoi_id, slave_acqs)


        slave_grouped_matched = util.group_acqs_by_orbit_number(slave_acqs)
                 
        orbitnumber_pv = {}
        selected_slave_acqs_by_orbitnumber = {}
        logger.info("\n\n\nTRACK : %s" %track)
        rejected_slave_orbitnumber = []
        for slave_orbitnumber in sorted( slave_grouped_matched["grouped"][track], reverse=True):
            selected_slave_acqs=[]
            selected = gtUtil.water_mask_check(slave_grouped_matched["acq_info"], slave_grouped_matched["grouped"][track][slave_orbitnumber],  aoi_location)
            if not selected:
                logger.info("Removing the acquisitions of orbitnumber : %s for failing water mask test" %slave_orbitnumber)
                rejected_slave_orbitnumber.append(slave_orbitnumber)
                continue
            pv_list = []
            for pv in slave_grouped_matched["grouped"][track][slave_orbitnumber]:
                logger.info("\tpv : %s" %pv)
                pv_list.append(pv)
                slave_ids= slave_grouped_matched["grouped"][track][slave_orbitnumber][pv]
                for slave_id in slave_ids:
                    selected_slave_acqs.append(slave_grouped_matched["acq_info"][slave_id])
            orbitnumber_pv[slave_orbitnumber] = len(list(set(pv_list)))
            selected_slave_acqs_by_orbitnumber[slave_orbitnumber] =  selected_slave_acqs

        for slave_orbitnumber in sorted( selected_slave_acqs_by_orbitnumber.keys(), reverse=True):
            slave_ipf_count = orbitnumber_pv[slave_orbitnumber]
            slave_acqs = selected_slave_acqs_by_orbitnumber[slave_orbitnumber]
            

            result, orbit_candidate_pair = process_enumeration(master_acqs, master_ipf_count, slave_acqs, slave_ipf_count, reject_pairs)            
            if result:
                candidate_pair_list.append(orbit_candidate_pair)
                min_max_count = min_max_count + 1
                if min_max_count>=MIN_MAX:
                    return min_max_count, candidate_pair_list
    return min_max_count, candidate_pair_list
      
    '''
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
                                
                                query = util.get_overlapping_slaves_query_orbit(acq)
                                query = util.get_overlapping_masters_query(master, acq):
                                slave_acqs = [i['fields']['partial'][0] for i in util.query_es2(query, es_index)]
                                logger.info("Found {} slave acqs : {}".format(len(acqs),
                                json.dumps([i['id'] for i in acqs], indent=2)))
                                matched_slave_acqs = util.create_acqs_from_metadata(slave_acqs)
                                


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
    '''

def get_master_slave_union_data(ref_acq, matched_acqs, acq_dict):
    """Return polygon of union of acquisition footprints."""

    starttimes = []
    endtimes = []

    starttimes.append(util.get_time(ref_acq.starttime))
    endtimes.append(util.get_time(ref_acq.endtime))

    for acq in matched_acqs:
        if acq.acq_id in acq_dict.keys():
            starttimes.append(util.get_time(acq.starttime))
            endtimes.append(util.get_time(acq.endtime))
    
    acq_dict[ref_acq.acq_id] = ref_acq.location

    starttime = sorted(starttimes)[0]
    endtime = sorted(endtimes, reverse=True)[0]

    return get_union_geometry(acq_dict), starttime.strftime("%Y-%m-%dT%H:%M:%S"), endtime.strftime("%Y-%m-%dT%H:%M:%S")

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
    master_slave_union_loc = None
    overlapped_matches = util.find_overlap_match(ref_acq, matched_acqs)
    if len(overlapped_matches)>0:
        overlapped_acqs = []
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
        for acq_id in overlapped_matches.keys():
            overlapped_acqs.append(acq_id[0])
        if is_overlapped and overlap>=0.98: # and overlap >=covth:
            logger.info("MATCHED")
            matched = True
            pair_union_loc, starttime, endtime  = get_master_slave_union_data(ref_acq, matched_acqs, overlapped_matches)
            if ref_type == "master":
                candidate_pair = {"master_acqs" : [ref_acq.acq_id[0]], "slave_acqs" : overlapped_acqs, "union_geojson" : pair_union_loc, "starttime" : starttime, "endtime" : endtime}
            else:
                candidate_pair = {"master_acqs" : overlapped_acqs, "slave_acqs" : [ref_acq.acq_id[0]], "union_geojson" : pair_union_loc, "starttime" : starttime, "endtime" : endtime}
    return matched, candidate_pair
            


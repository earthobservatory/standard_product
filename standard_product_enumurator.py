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
import dateutil.parser
import pickle

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
ACQ_LIST_ID_TMPL = "acq-list_R{}_M{:d}S{:d}_TN{:03d}_{:%Y%m%dT%H%M%S}-{:%Y%m%dT%H%M%S}-{}-{}"


BASE_PATH = os.path.dirname(__file__)
GRQ_ES_URL = "http://100.64.134.208:9200/"
covth = 0.98
MIN_MATCH = 100
es_index = "grq_*_*acquisition*"

def get_orbit_date(s):
    date = dateutil.parser.parse(s, ignoretz=True)
    date = date.replace(minute=0, hour=12, second=0)
    return date.isoformat()


def query_es(query, es_index=None):
    """Query ES."""
    es_url = "http://100.64.134.208:9200/"
    #es_url = app.conf.GRQ_ES_URL
    rest_url = es_url[:-1] if es_url.endswith('/') else es_url
    url = "{}/_search?search_type=scan&scroll=60&size=100".format(rest_url)
    if es_index:
        url = "{}/{}/_search?search_type=scan&scroll=60&size=100".format(rest_url, es_index)
    #logger.info("url: {}".format(url))
    r = requests.post(url, data=json.dumps(query))
    r.raise_for_status()
    scan_result = r.json()
    #logger.info("scan_result: {}".format(json.dumps(scan_result, indent=2)))
    count = scan_result['hits']['total']
    scroll_id = scan_result['_scroll_id']
    hits = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' % rest_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        hits.extend(res['hits']['hits'])
    return hits

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

def get_aoi_blacklist_data(aoi):
    es_index = "grq_*_blacklist"
    query = {
       "query": {
        "filtered": {
            "query": {

              "bool": {
                "must": [
                  {
                    "match": {
                      "dataset_type": "ifg_cfg_blacklist"
                      }
                  }
                ]
              }
            },
            "filter": {
              "geo_shape": {
                "location": {
                  "shape": aoi['location']
                }
              }
            }
          }
        },
        "partial_fields" : {
          "partial" : {
            "include" : [ "id", "starttime", "endtime", "location", 
                              "metadata.user_tags", "metadata.priority" ]
          }
        }
      }
    


    print(query)
    bls = [i['fields']['partial'][0] for i in query_es(query, es_index)]
    print("Found {} bls for {}: {}".format(len(bls), aoi['id'],
                    json.dumps([i['id'] for i in bls], indent=2)))

    #print("ALL ACQ of AOI : \n%s" %acqs)
    if len(acqs) <=0:
        print("No blacklist there for AOI : %s" %aoi['id'])
    return bls

def gen_hash(master_scenes, slave_scenes):
    '''Generates a hash from the master and slave scene list''' 
    master = [x.replace('acquisition-', '') for x in master_scenes]
    slave = [x.replace('acquisition-', '') for x in slave_scenes]
    master = pickle.dumps(sorted(master))
    slave = pickle.dumps(sorted(slave))
    return '{}_{}'.format(hashlib.md5(master).hexdigest(), hashlib.md5(slave).hexdigest())





def get_aoi_blacklist(aoi):
    bl_array = []  
    bls = get_aoi_blacklist_data(aoi)
    for bl in bls:
        if 'master_scenes' in bl['metadata']:
            master_scenes = bl['metadata']['master_scenes']
            slave_scenes = bl['metadata']['slave_scenes']
            bl_array.append(gen_hash(master_scenes, slave_scenes))
        else:
            logger.warn("MASTER SCENES not Found in BL")

    return bl_array
    

def print_groups(grouped_matched):
    for track in grouped_matched["grouped"]:
        logger.info("\nTrack : %s" %track)
        for day_dt in sorted(grouped_matched["grouped"][track], reverse=True):
            logger.info("\tDate : %s" %day_dt)
            for pv in grouped_matched["grouped"][track][day_dt]:

                for acq in grouped_matched["grouped"][track][day_dt][pv]:
                    logger.info("\t\t%s : %s" %(pv, acq[0]))

def enumerate_acquisations(orbit_acq_selections):


    logger.info("\n\n\nENUMERATE\n")
    job_data = orbit_acq_selections["job_data"]
    orbit_aoi_data = orbit_acq_selections["orbit_aoi_data"]
    orbit_data = orbit_acq_selections["orbit_data"]

    aoi_blacklist = []
    orbit_file = orbit_data['orbit_file']

    candidate_pair_list = []

    for aoi_id in orbit_aoi_data.keys():
        logger.info("\nenumerate_acquisations : Processing AOI : %s " %aoi_id)
        aoi_data = orbit_aoi_data[aoi_id]
        aoi_blacklist = get_aoi_blacklist(aoi_data)
        logger.info("BlackList for AOI %s:\n\t%s" %(aoi_id, aoi_data))
        
        selected_track_acqs = aoi_data['selected_track_acqs'] 
        #logger.info("%s : %s\n" %(aoi_id, selected_track_acqs))

        for track in selected_track_acqs.keys():
            logger.info("\nenumerate_acquisations : Processing track : %s " %track)
            if len(selected_track_acqs[track].keys()) <=0:
                logger.info("\nenumerate_acquisations : No selected data for track : %s " %track)
                continue
            min_max_count, track_candidate_pair_list = get_candidate_pair_list(aoi_id, track, selected_track_acqs[track], aoi_data, orbit_data, aoi_blacklist)
            logger.info("\n\nAOI ID : %s MIN MAX count for track : %s = %s" %(aoi_id, track, min_max_count))
            if min_max_count>0:
                print_candidate_pair_list_per_track(track_candidate_pair_list)
            if min_max_count >= MIN_MATCH and len(track_candidate_pair_list) > 0:
                candidate_pair_list.extend(track_candidate_pair_list)

    return candidate_pair_list


def black_list_check(candidate_pair, black_list):
    passed = False
    master_acquisitions = candidate_pair["master_acqs"]
    slave_acquisitions = candidate_pair["slave_acqs"]
    ifg_hash = gen_hash(master_acquisitions, slave_acquisitions)
    if ifg_hash not in black_list:
        passed = True
        logger.info("black_list_check : ifg_hash %s not in blackl_list. So PASSING")
    else:
        logger.info("black_list_check : ifg_hash %s IS in blackl_list. So FAILING") 
        passed = False
    return passed

def get_candidate_pair_list2(selected_track_acqs, aoi_blacklist):
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
        result, orbit_candidate_pair_list = process_enumeration(master_acqs, master_ipf_count, slave_acqs, slave_ipf_count, aoi_blacklist)
        if result and len(orbit_candidate_pair_list)>0:
            candidate_pair_list.extend(orbit_candidate_pair_list)
            min_max_count = min_max_count + 1
            if min_max_count>=MIN_MATCH:
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
                result, orbit_candidate_pair_list = process_enumeration(master_acqs, master_ipf_count, slave_acqs, slave_ipf_count, aoi_blacklist)
                if result and len(orbit_candidate_pair_list)>0:
                    candidate_pair_list.extend(orbit_candidate_pair_list)
                    min_max_count = min_max_count + 1
                    if min_max_count>=MIN_MATCH:
                        return min_max_count, candidate_pair_list
    return min_max_count, candidate_pair_list
    
def process_enumeration(master_acqs, master_ipf_count, slave_acqs, slave_ipf_count, aoi_location, aoi_blacklist):
    result = False
    candidate_pair_list = []
    
    #master_ipf_count=2

    logger.info("Master IPF Count : %s and Slave IPF Count : %s" %(master_ipf_count, slave_ipf_count)) 
    ref_type = None

    if slave_ipf_count == 1:
        logger.info("process_enumeration : Ref : Master, #of acq : %s" %len(master_acqs))
        for acq in master_acqs:
            logger.info("Running CheckMatch for Master acq : %s" %acq.acq_id)
            result, candidate_pair = check_match(acq, slave_acqs, aoi_location, "master") 
            if not result:
                logger.info("CheckMatch Failed. So Returning False")
                return False, []
            elif black_list_check(candidate_pair, aoi_blacklist):
                candidate_pair_list.append(candidate_pair)
                logger.info("process_enumeration: CheckMatch Passed. Adding candidate pair: ")
                print_candidate_pair(candidate_pair)
    elif slave_ipf_count > 1 and master_ipf_count == 1:
        logger.info("process_enumeration : Ref : Slave, #of acq : %s" %len(slave_acqs))
        for acq in slave_acqs:
            logger.info("Running CheckMatch for Slave acq : %s" %acq.acq_id)
            result, candidate_pair = check_match(acq, master_acqs, aoi_location, "slave")         
            if not result:
                logger.info("CheckMatch Failed. So Returning False")
                return False, []
            elif black_list_check(candidate_pair, aoi_blacklist):
                candidate_pair_list.append(candidate_pair)
                print_candidate_pair(candidate_pair)
    else:
        logger.warn("No Selection as both Master and Slave has multiple ipf")

    if len(candidate_pair_list) == 0:
        result = False
    else:
        result = True
    return result, candidate_pair_list


def enumerate_acquisations(orbit_acq_selections):

    global MIN_MATCH

    logger.info("\n\n\nENUMERATE\n")
    #logger.info("orbit_dt : %s" %orbit_dt)
    job_data = orbit_acq_selections["job_data"]
    MIN_MATCH = job_data['minMatch']
    threshold_pixel = job_data['threshold_pixel']
    orbit_aoi_data = orbit_acq_selections["orbit_aoi_data"]
    orbit_data = orbit_acq_selections["orbit_data"]
    aoi_blacklist = []
    orbit_file = job_data['orbit_file']

    #candidate_pair_list = []

    for aoi_id in orbit_aoi_data.keys():
        try:
            candidate_pair_list = []
            logger.info("\nenumerate_acquisations : Processing AOI : %s " %aoi_id)
            aoi_data = orbit_aoi_data[aoi_id]
            selected_track_acqs = aoi_data['selected_track_acqs']
            #logger.info("%s : %s\n" %(aoi_id, selected_track_acqs))

            for track in selected_track_acqs.keys():
                logger.info("\nenumerate_acquisations : Processing track : %s " %track)
                if len(selected_track_acqs[track].keys()) <=0:
                    logger.info("\nenumerate_acquisations : No selected data for track : %s " %track)
                    continue
                min_max_count, track_candidate_pair_list = get_candidate_pair_list(aoi_id, track, selected_track_acqs[track], aoi_data, orbit_data, aoi_blacklist, threshold_pixel)
                logger.info("\n\nAOI ID : %s MIN MAX count for track : %s = %s" %(aoi_id, track, min_max_count))
                if min_max_count>0:
                    print_candidate_pair_list_per_track(track_candidate_pair_list)
                if len(track_candidate_pair_list) > 0:
                    for track_dt_list in track_candidate_pair_list:
                        candidate_pair_list.extend(track_dt_list)
            if len(candidate_pair_list)>0:
                logger.info("\nPublishing ACQ List for AOI : %s" %aoi_id)
                publish_initiator(candidate_pair_list, job_data)
            else:
                logger.info("\nNOTHING to publish for AOI : %s" %aoi_id)
        except Exception as err:
            logger.warn("Error with Enumeration for aoi : %s : %s" %(aoi_id, str(err)))
            logger.warn("Traceback: {}".format(traceback.format_exc()))
                  
    #return candidate_pair_list

def print_candidate_pair_list_per_track(track_candidate_pair_list):
    for track_dt_list in track_candidate_pair_list:
        for candidate_pair in track_dt_list:
            logger.info("Masters Acqs:")
            print_candidate_pair(candidate_pair)
            #logger.info("print_candidate_pair_list_per_track : %s : %s " %(type(candidate_pair), candidate_pair))
            #logger.info("Masters Acqs:")
            #logger.info(candidate_pair["master_acqs"])

            '''
            for j in range(len(candidate_pair["master_acqs"])):
                logger.info(candidate_pair["master_acqs"][j])
            for j in range(len(candidate_pair["slave_acqs"])):
                logger.info(candidate_pair["slave_acqs"][j])
            '''


def print_candidate_pair(candidate_pair):
    logger.info("Master : ")
    for master_acq in candidate_pair["master_acqs"]:
        logger.info(master_acq)
    logger.info("Slave : ")
    for master_acq in candidate_pair["slave_acqs"]: 
        logger.info(master_acq)



def get_candidate_pair_list(aoi, track, selected_track_acqs, aoi_data, orbit_data, aoi_blacklist, threshold_pixel):
    logger.info("get_candidate_pair_list : %s Orbits" %len(selected_track_acqs.keys()))
    candidate_pair_list = []
    orbit_ipf_dict = {}
    min_max_count = 0
    aoi_location = aoi_data['aoi_location']
    logger.info("aoi_location : %s " %aoi_location)

    for track_dt in sorted(selected_track_acqs.keys(), reverse=True):
        logger.info(track_dt)
   
        slaves_track = {}
        slave_acqs = []
            
        master_acqs = selected_track_acqs[track_dt]
        master_ipf_count, master_starttime, master_endtime, master_location, master_track, direction, master_orbitnumber = util.get_union_data_from_acqs(master_acqs)
        #master_ipf_count = util.get_ipf_count(master_acqs)
        #master_union_geojson = util.get_union_geojson_acqs(master_acqs)

        #util.print_acquisitions(aoi_data['aoi_id'], master_acqs)
        query = util.get_overlapping_slaves_query(util.get_isoformat_date(master_starttime), aoi_location, track, direction, orbit_data['platform'], master_orbitnumber)
        logger.info("Slave Finding Query : %s" %query)
        
        acqs = [i['fields']['partial'][0] for i in util.query_es2(query, es_index)]
        logger.info("Found {} slave acqs : {}".format(len(acqs),
        json.dumps([i['id'] for i in acqs], indent=2)))


        if len(acqs) == 0:
            logger.info("ERROR ERROR : NO SLAVE FOUND for AOI %s and track %s" %(aoi_data['aoi_id'], track))
            continue

        #matched_acqs = util.create_acqs_from_metadata(process_query(query))
        slave_acqs = util.create_acqs_from_metadata(acqs)
        logger.info("\nSLAVE ACQS")
        #util.print_acquisitions(aoi_id, slave_acqs)


        slave_grouped_matched = util.group_acqs_by_track_date(slave_acqs)        
        logger.info("Priniting Slaves")
        print_groups(slave_grouped_matched)
        track_dt_pv = {}
        selected_slave_acqs_by_track_dt = {}
        logger.info("\n\n\nTRACK : %s" %track)
        rejected_slave_track_dt = []
        for slave_track_dt in sorted( slave_grouped_matched["grouped"][track], reverse=True):
            selected_slave_acqs=[]
            orbit_file = None
            orbit_dt = slave_track_dt.replace(minute=0, hour=12, second=0).isoformat()
            logger.info("\n\n\nProcessing AOI: %s Track : %s  orbit_dt : %s" %(aoi, track, orbit_dt))

            isOrbitFile, orbit_id, orbit_url = util.get_orbit_file(orbit_dt, orbit_data['platform'])
            if isOrbitFile:
                logger.info("%s : %s" %(orbit_id, orbit_url))
                slave_orbit_file_path = os.path.basename(orbit_url)
                downloaded = gtUtil.download_orbit_file(orbit_url, slave_orbit_file_path)
                if downloaded:
                    logger.info("Slave Orbiut File Downloaded")
                    orbit_file = slave_orbit_file_path
            if orbit_file:
                logger.info("Orbit File Exists, so Running water_mask_check for slave for date %s is running with orbit file : %s " %(slave_track_dt, orbit_file))
                selected = gtUtil.water_mask_check(track, slave_track_dt, slave_grouped_matched["acq_info"], slave_grouped_matched["grouped"][track][slave_track_dt],  aoi_location, aoi, threshold_pixel, orbit_file)
                if not selected:
                    logger.info("Removing the acquisitions of orbitnumber : %s for failing water mask test" %slave_track_dt)
                    rejected_slave_track_dt.append(slave_track_dt)
                    continue
            else:
                logger.info("Orbit File NOT Exists, so Running water_mask_check for slave for date %s is running without orbit file." %slave_track_dt)
                selected = gtUtil.water_mask_check(track, slave_track_dt, slave_grouped_matched["acq_info"], slave_grouped_matched["grouped"][track][slave_track_dt],  aoi_location, aoi, threshold_pixel)
                if not selected:
                    logger.info("Removing the acquisitions of orbitnumber : %s for failing water mask test" %slave_track_dt)
                    rejected_slave_track_dt.append(slave_track_dt)
                    continue
            pv_list = []
            for pv in slave_grouped_matched["grouped"][track][slave_track_dt]:
                logger.info("\tpv : %s" %pv)
                pv_list.append(pv)
                slave_ids= slave_grouped_matched["grouped"][track][slave_track_dt][pv]
                for slave_id in slave_ids:
                    selected_slave_acqs.append(slave_grouped_matched["acq_info"][slave_id])
            slave_ipf_count = len(list(set(pv_list)))
            track_dt_pv[slave_track_dt] = slave_ipf_count
            selected_slave_acqs_by_track_dt[slave_track_dt] =  selected_slave_acqs

        #for slave_track_dt in sorted( selected_slave_acqs_by_track_dt.keys(), reverse=True):
            #slave_ipf_count = track_dt_pv[slave_track_dt]
            logger.info("Processing Slaves with date : %s" %slave_track_dt)
            #if not slave_track_dt == "2016-02-03 00:00:00":
                #logger.info("REJECTING foir Test")
                #continue
            #slave_acqs = selected_slave_acqs_by_track_dt[slave_track_dt]
            

            result, orbit_candidate_pair = process_enumeration(master_acqs, master_ipf_count, selected_slave_acqs, slave_ipf_count, aoi_location, aoi_blacklist)            
            if result:
                candidate_pair_list.append(orbit_candidate_pair)
                min_max_count = min_max_count + 1
                if min_max_count>=MIN_MATCH:
                    return min_max_count, candidate_pair_list
    return min_max_count, candidate_pair_list



def get_candidate_pair_list_by_orbitnumber(track, selected_track_acqs, aoi_data, orbit_data, aoi_blacklist, threshold_pixel):
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
        query = util.get_overlapping_slaves_query(orbit_data, aoi_location, track, direction, master_orbitnumber)

        acqs = [i['fields']['partial'][0] for i in util.query_es2(query, es_index)]
        logger.info("Found {} slave acqs : {}".format(len(acqs),
        json.dumps([i['id'] for i in acqs], indent=2)))


                    #matched_acqs = util.create_acqs_from_metadata(process_query(query))
        slave_acqs = util.create_acqs_from_metadata(acqs)
        logger.info("\nSLAVE ACQS")
        #util.print_acquisitions(aoi_id, slave_acqs)


        slave_grouped_matched = util.group_acqs_by_orbit_number(slave_acqs)
        #slave_grouped_matched = util.group_acqs_by_track_date(slave_acqs)
         
        orbitnumber_pv = {}
        selected_slave_acqs_by_orbitnumber = {}
        logger.info("\n\n\nTRACK : %s" %track)
        rejected_slave_orbitnumber = []
        for slave_orbitnumber in sorted( slave_grouped_matched["grouped"][track], reverse=True):
            selected_slave_acqs=[]
            selected = gtUtil.water_mask_check(track, slave_orbitnumber, slave_grouped_matched["acq_info"], slave_grouped_matched["grouped"][track][slave_orbitnumber],  aoi_location, aoi, threshold_pixel)
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
            

            result, orbit_candidate_pair = process_enumeration(master_acqs, master_ipf_count, slave_acqs, slave_ipf_count, aoi_location, aoi_blacklist)            
            if result:
                candidate_pair_list.append(orbit_candidate_pair)
                min_max_count = min_max_count + 1
                if min_max_count>=MIN_MATCH:
                    return min_max_count, candidate_pair_list
    return min_max_count, candidate_pair_list
      
def get_master_slave_intersect_data(ref_acq, matched_acqs, acq_dict):
    """Return polygon of union of acquisition footprints."""

    union_geojson = get_union_geometry(acq_dict)
    intersect_geojson, int_env = util.get_intersection(ref_acq.location, union_geojson)

    return intersect_geojson, starttime.strftime("%Y-%m-%dT%H:%M:%S"), endtime.strftime("%Y-%m-%dT%H:%M:%S")

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

def check_match(ref_acq, matched_acqs, aoi_location, ref_type = "master"):
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
        is_overlapped = True
        overlap = 0
        try:
            is_overlapped, overlap = util.find_overlap_within_aoi(ref_acq.location, union_loc, aoi_location)
        except Exception as err:
            logger.warn(str(err))
            logger.warn("Traceback: {}".format(traceback.format_exc()))

        logger.info("is_ref_truncated : %s" %is_ref_truncated)
        logger.info("is_within : %s" %is_covered)
        logger.info("is_overlapped : %s, overlap : %s" %(is_overlapped, overlap))
        for acq_id in overlapped_matches.keys():
            overlapped_acqs.append(acq_id[0])
        if overlap <=0.98 or not is_overlapped:
            logger.info("ERROR ERROR, overlap is %s " %overlap)
        if is_overlapped: # and overlap>=0.98: # and overlap >=covth:
            logger.info("MATCHED")
            matched = True
            starttime = ref_acq.starttime
            endtime = ref_acq.endtime
            pair_intersection_loc, pair_intersection_env = util.get_intersection(ref_acq.location, union_loc)
            if ref_type == "master":
                candidate_pair = {"master_acqs" : [ref_acq.acq_id[0]], "slave_acqs" : overlapped_acqs, "intersect_geojson" : pair_intersection_loc, "starttime" : starttime, "endtime" : endtime}
            else:
                candidate_pair = {"master_acqs" : overlapped_acqs, "slave_acqs" : [ref_acq.acq_id[0]], "intersect_geojson" : pair_intersection_loc, "starttime" : starttime, "endtime" : endtime}
    return matched, candidate_pair
            
def publish_initiator(candidate_pair_list, job_data):
    for candidate_pair in candidate_pair_list:
        publish_initiator_pair(candidate_pair, job_data)


def publish_initiator_pair(candidate_pair, job_data, wuid=None, job_num=None):
  
    master_ids_str=""
    slave_ids_str=""
    job_priority = 0

    master_acquisitions = candidate_pair["master_acqs"]
    slave_acquisitions = candidate_pair["slave_acqs"]
    union_geojson = candidate_pair["intersect_geojson"]
    starttime = candidate_pair["starttime"]
    endtime = candidate_pair["endtime"]


    project = job_data["project"] 
    '''
    spyddder_extract_version = job_data["spyddder_extract_version"] 
    standard_product_ifg_version = job_data["standard_product_ifg_version"] 
    acquisition_localizer_version = job_data["acquisition_localizer_version"]
    standard_product_localizer_version = job_data["standard_product_localizer_version"] 
    '''
    #job_data["job_type"] = job_type
    #job_data["job_version"] = job_version
    job_priority = job_data["job_priority"] 


    logger.info("MASTER : %s " %master_acquisitions)
    logger.info("SLAVE : %s" %slave_acquisitions) 
    logger.info("project: %s" %project)

    #version = get_version()
    version = "v2.0.0"

    # set job type and disk space reqs
    disk_usage = "300GB"

    # query docs
    es_url = GRQ_ES_URL
    grq_index_prefix = "grq"
    rest_url = es_url[:-1] if es_url.endswith('/') else es_url
    url = "{}/{}/_search?search_type=scan&scroll=60&size=100".format(rest_url, grq_index_prefix)

    # get metadata
    master_md = { i:util.get_metadata(i, rest_url, url) for i in master_acquisitions }
    #logger.info("master_md: {}".format(json.dumps(master_md, indent=2)))
    slave_md = { i:util.get_metadata(i, rest_url, url) for i in slave_acquisitions }
    #logger.info("slave_md: {}".format(json.dumps(slave_md, indent=2)))

    # get tracks
    track = util.get_track(master_md)
    logger.info("master_track: {}".format(track))
    slave_track = util.get_track(slave_md)
    logger.info("slave_track: {}".format(slave_track))
    if track != slave_track:
        raise RuntimeError("Slave track {} doesn't match master track {}.".format(slave_track, track))

    ref_scence = master_md
    if len(master_acquisitions)==1:
        ref_scence = master_md
    elif len(slave_acquisitions)==1:
        ref_scence = slave_md
    elif len(master_acquisitions) > 1 and  len(slave_acquisitions)>1:
        raise RuntimeError("Single Scene Reference Required.")
 

    dem_type = util.get_dem_type(master_md)

    # get dem_type
    dem_type = util.get_dem_type(master_md)
    logger.info("master_dem_type: {}".format(dem_type))
    slave_dem_type = util.get_dem_type(slave_md)
    logger.info("slave_dem_type: {}".format(slave_dem_type))
    if dem_type != slave_dem_type:
        dem_type = "SRTM+v3"


 
    job_queue = "%s-job_worker-large" % project
    logger.info("submit_localize_job : Queue : %s" %job_queue)

    #localizer_job_type = "job-standard_product_localizer:%s" % standard_product_localizer_version

    logger.info("master acq type : %s of length %s"  %(type(master_acquisitions), len(master_acquisitions)))
    logger.info("slave acq type : %s of length %s" %(type(slave_acquisitions), len(master_acquisitions)))

    if type(project) is list:
        project = project[0]


    for acq in master_acquisitions:
        #logger.info("master acq : %s" %acq)
        if master_ids_str=="":
            master_ids_str= acq
        else:
            master_ids_str += " "+acq

    for acq in slave_acquisitions:
        #logger.info("slave acq : %s" %acq)
        if slave_ids_str=="":
            slave_ids_str= acq
        else:
            slave_ids_str += " "+acq

    list_master_dt, list_slave_dt = util.get_acq_dates(master_acquisitions, slave_acquisitions)
    
    #ACQ_LIST_ID_TMPL = "acq_list-R{}_M{:d}S{:d}_TN{:03d}_{:%Y%m%dT%H%M%S}-{:%Y%m%dT%H%M%S}-{}-{}"
    
    id_hash = hashlib.md5(json.dumps([
            job_priority,
            master_ids_str,
            slave_ids_str,
            dem_type
    ]).encode("utf8")).hexdigest()


    '''
    id_hash = hashlib.md5(json.dumps([
        ACQ_LIST_ID_TMPL,
        m,
        master_orbit_urls[-1],
        slave_zip_urls[-1],
        slave_orbit_urls[-1],
        projects[-1],
        filter_strength,
	dem_type
    ]).encode("utf8")).hexdigest()
    '''

    orbit_type = 'poeorb'

    id = ACQ_LIST_ID_TMPL.format('M', len(master_acquisitions), len(slave_acquisitions), track, list_master_dt, list_slave_dt, orbit_type, id_hash[0:4])
    #id = "acq-list-%s" %id_hash[0:4]
    prod_dir =  id
    os.makedirs(prod_dir, 0o755)

    met_file = os.path.join(prod_dir, "{}.met.json".format(id))
    ds_file = os.path.join(prod_dir, "{}.dataset.json".format(id))
  
    #with open(met_file) as f: md = json.load(f)
    md = {}
    md['id'] = id
    md['project'] =  project,
    md['master_acquisitions'] = master_ids_str
    md['slave_acquisitions'] = slave_ids_str
    '''
    md['spyddder_extract_version'] = spyddder_extract_version
    md['acquisition_localizer_version'] = acquisition_localizer_version
    md['standard_product_ifg_version'] = standard_product_ifg_version
    '''
    md['job_priority'] = job_priority
    md['_disk_usage'] = disk_usage
    md['soft_time_limit'] =  86400
    md['time_limit'] = 86700
    md['dem_type'] = dem_type
    md['track'] = track
    md['starttime'] = "%sZ" %starttime
    md['endtime'] = "%sZ" %endtime
    md['union_geojson'] = union_geojson
    md['master_scenes'] = master_acquisitions 
    md['slave_scenes'] = slave_acquisitions


 
    try:
        geom = ogr.CreateGeometryFromJson(json.dumps(union_geojson))
        env = geom.GetEnvelope()
        bbox = [
            [ env[3], env[0] ],
            [ env[3], env[1] ],
            [ env[2], env[1] ],
            [ env[2], env[0] ],
        ]     
        md['bbox'] = bbox
    except Exception as e:
        logger.warn("Got exception creating bbox : {}".format( str(e)))
        logger.warn("Traceback: {}".format(traceback.format_exc()))

    with open(met_file, 'w') as f: json.dump(md, f, indent=2)

    print("creating dataset file : %s" %ds_file)
    util.create_dataset_json(id, version, met_file, ds_file)


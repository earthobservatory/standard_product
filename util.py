#!/usr/bin/env python 
import os, sys, time, json, requests, logging
import re, traceback, argparse, copy, bisect
from xml.etree import ElementTree
#from hysds_commons.job_utils import resolve_hysds_job
#from hysds.celery import app
from shapely.geometry import Polygon
from shapely.ops import cascaded_union
import datetime
import dateutil.parser
from datetime import datetime, timedelta
import groundTrack
from osgeo import ogr, osr
import lightweight_water_mask


GRQ_URL="http://100.64.134.208:9200/"

logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logger.setLevel(logging.INFO)
#logger.addFilter(LogFilter())

SLC_RE = re.compile(r'(?P<mission>S1\w)_IW_SLC__.*?' +
                    r'_(?P<start_year>\d{4})(?P<start_month>\d{2})(?P<start_day>\d{2})' +
                    r'T(?P<start_hour>\d{2})(?P<start_min>\d{2})(?P<start_sec>\d{2})' +
                    r'_(?P<end_year>\d{4})(?P<end_month>\d{2})(?P<end_day>\d{2})' +
                    r'T(?P<end_hour>\d{2})(?P<end_min>\d{2})(?P<end_sec>\d{2})_.*$')

BASE_PATH = os.path.dirname(__file__)
MISSION = 'S1A'


class ACQ:
    def __init__(self, acq_id, download_url, tracknumber, location, starttime, endtime, direction, orbitnumber, identifier, pv ):
        self.acq_id=acq_id,
        self.download_url = download_url
        self.tracknumber = tracknumber
        self.location= location
        self.starttime = starttime
        self.endtime = endtime
        self.pv = pv
        self.direction = direction
        self.orbitnumber = orbitnumber
        self.identifier = identifier
        
        #print("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s" %(acq_id, download_url, tracknumber, location, starttime, endtime, direction, orbitnumber, identifier, pv))



# set logger
log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


BASE_PATH = os.path.dirname(__file__)
MOZART_ES_ENDPOINT = "MOZART"
GRQ_ES_ENDPOINT = "GRQ"

def print_acq(acq):
    logger.info("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s" %(acq.acq_id, acq.download_url, acq.tracknumber, acq.location, acq.starttime, acq.endtime, acq.direction, acq.orbitnumber, acq.identifier, acq.pv))

def group_acqs_by_orbit_number_from_metadata(frames):
    return group_acqs_by_orbit_number(create_acqs_from_metadata(frames))

def group_acqs_by_orbit_number(acqs):
    #logger.info(acqs)
    grouped = {}
    acqs_info = {}
    for acq in acqs:
        acqs_info[acq.acq_id] = acq
        bisect.insort(grouped.setdefault(acq.tracknumber, {}).setdefault(acq.orbitnumber, {}).setdefault(acq.pv, []), acq.acq_id)
        '''
        if track in grouped.keys():
            if orbitnumber in grouped[track].keys():
                if pv in grouped[track][orbitnumber].keys():
                    grouped[track][orbitnumber][pv] = grouped[track][orbitnumber][pv].append(slave_acq)
                else:
                    slave_acqs = [slave_acq]
                    slave_pv = {}
                
                    grouped[track][orbitnumber] = 
        '''
    return {"grouped": grouped, "acq_info" : acqs_info}

def update_acq_pv(acq_id, pv):
    pass


def water_mask_check(acq_info, grouped_matched_orbit_number,  aoi_location, orbit_file=None):

    result = False
    if not aoi_location:
        logger.info("water_mask_check FAILED as aoi_location NOT found")
        return False
    try:
        result = water_mask_test1(acq_info, grouped_matched_orbit_number,  aoi_location, orbit_file)
    except Exception as err:
        traceback.print_exc()
    return result




def water_mask_test1(acq_info, grouped_matched_orbit_number,  aoi_location, orbit_file = None):

    passed = False
    starttimes = []
    endtimes = []
    polygons = []
    acqs_land = []
    acqs_water = []
    for pv in grouped_matched_orbit_number:
        acq_ids = grouped_matched_orbit_number[pv]
        for acq_id in acq_ids:
            logger.info("%s : %s" %(pv, acq_id))
            acq = acq_info[acq_id]
            starttimes.append(get_time(acq.starttime))
            endtimes.append(get_time(acq.endtime)) 
            polygons.append(acq.location)
            if orbit_file:
                land, water = get_area_from_orbit_file(get_time(acq.starttime), get_time(acq.endtime), orbit_file, aoi_location)
                acqs_land.append(land)
                acqs_water.append(water)
            else:
                land, water = get_area_from_acq_location(acq.location, aoi_location)
                acqs_land.append(land)
                acqs_water.append(water)
              
    total_land = 0
    total_water = 0
    
    if orbit_file:

        logger.info("starttimes : %s" %starttimes)
        logger.info("endtimes : %s" %endtimes)
        #get lowest starttime minus 10 minutes as starttime
        tstart = getUpdatedTime(sorted(starttimes)[0], -10)
        logger.info("tstart : %s" %tstart)
        tend = getUpdatedTime(sorted(endtimes, reverse=True)[0], 10)
        logger.info("tend : %s" %tend)
        total_land, total_water = get_area_from_orbit_file(tstart, tend, orbit_file, aoi_location)
    else:        
        union_geojson = get_union_geometry(polygons)
        logger.info("union_geojson : %s" %union_geojson)
        #intersection, int_env = get_intersection(aoi['location'], union_geojson)
        #logger.info("union intersection : %s" %intersection)
        total_land, total_water = get_area_from_orbit_file(union_geojson, aoi_location)
    


    #ADD THE SELECTION LOGIC HERE

    passed = False
    passed = isTrackSelected(acqs_land, total_land)
    return passed

def isTrackSelected(acqs_land, total_land):
    selected = False
    sum_of_acq_land = 0

    for acq_land in acqs_land:
        sum_of_acq_land+= acq_land

    delta = abs(sum_of_acq_land - total_land)
    if delta/total_land<.01:
        selected = True

    return selected

def get_ipf_count(acqs):
    pv_list = []
    for acq in acqs:
        if acq.pv:
            pv_list.append(acq.pv)
        else:
            pv = get_processing_version(acq.identifier)
            if pv:
                update_grq(acq.acq_id, acq.pv)
                pv_list.append(pv)

    return len(list(set(pv_list)))

def get_union_data_from_acqs(acqs):
    starttimes = []
    endtimes = []
    polygons = []
    track = None
    direction = None
    orbitnumber = None
    pv_list = []

    for acq in acqs:
        if acq.pv:
            pv_list.append(acq.pv)
        else:
            pv = get_processing_version(acq.identifier)
            if pv:
                update_grq(acq.acq_id, acq.pv)
                pv_list.append(pv)

        starttimes.append(get_time(acq.starttime))
        endtimes.append(get_time(acq.endtime)) 
        polygons.append(acq.location)
        track = acq.tracknumber
        direction = acq.direction
        orbitnumber =acq.orbitnumber
    starttime = sorted(starttimes)[0]
    endtime = sorted(endtimes, reverse=True)[0]
    location = get_union_geometry(polygons)
    ipf_count = len(list(set(pv_list)))

    return ipf_count, starttime.strftime("%Y-%m-%d %H:%M:%S"), endtime.strftime("%Y-%m-%d %H:%M:%S"), location, track, direction, orbitnumber 


def create_acq_obj_from_metadata(acq):
    #create ACQ(acq_id, download_url, tracknumber, location, starttime, endtime, direction, orbitnumber, identifier, pv)
    #logger.info("ACQ = %s\n" %acq)
    acq_data = acq #acq['fields']['partial'][0]
    acq_id = acq['id']
    #print("acq_id : %s : %s" %(type(acq_id), acq_id))
    match = SLC_RE.search(acq_id)
    if not match:
        logger.info("Error : No Match : %s" %acq_id)
        return None
    download_url = acq_data['metadata']['download_url']
    track = acq_data['metadata']['trackNumber']
    location = acq_data['metadata']['location']
    starttime = acq_data['starttime']
    endtime = acq_data['endtime']
    direction = acq_data['metadata']['direction']
    orbitnumber = acq_data['metadata']['orbitNumber']
    identifier = acq_data['metadata']['identifier']
    pv = None
    if "processing_version" in  acq_data['metadata']:
        pv = acq_data['metadata']['processing_version']
    else:
        pv = get_processing_version(identifier)
        update_acq_pv(acq_id, pv) 
    return ACQ(acq_id, download_url, track, location, starttime, endtime, direction, orbitnumber, identifier, pv)


def create_acqs_from_metadata(frames):
    acqs = []
    #print("frame length : %s" %len(frames))
    for acq in frames:
        acq_obj = create_acq_obj_from_metadata(acq)
        if acq_obj:
            acqs.append(acq_obj)
    return acqs


def group_frames_by_track_date(frames):
    """Classify frames by track and date."""

    hits = {}
    grouped = {}
    dates = {}
    footprints = {}
    metadata = {}
    for h in frames: 
        if h['_id'] in hits: continue
        fields = h['fields']['partial'][0]
        #print("h['_id'] : %s" %h['_id'])

        # get product url; prefer S3
        prod_url = fields['urls'][0]
        if len(fields['urls']) > 1:
            for u in fields['urls']:
                if u.startswith('s3://'):
                    prod_url = u
                    break
        #print("prod_url : %s" %prod_url)
        hits[h['_id']] = "%s/%s" % (prod_url, fields['metadata']['archive_filename'])
        match = SLC_RE.search(h['_id'])
        #print("match : %s" %match)
        if not match:
            raise RuntimeError("Failed to recognize SLC ID %s." % h['_id'])
        day_dt = datetime(int(match.group('start_year')),
                          int(match.group('start_month')),
                          int(match.group('start_day')),
                          0, 0, 0)
        #print("day_dt : %s " %day_dt)

        bisect.insort(grouped.setdefault(fields['metadata']['trackNumber'], {}) \
                             .setdefault(day_dt, []), h['_id'])
        slc_start_dt = datetime(int(match.group('start_year')),
                                int(match.group('start_month')),
                                int(match.group('start_day')),
                                int(match.group('start_hour')),
                                int(match.group('start_min')),
                                int(match.group('start_sec')))
        #print("slc_start_dt : %s" %slc_start_dt)

        slc_end_dt = datetime(int(match.group('end_year')),
                              int(match.group('end_month')),
                              int(match.group('end_day')),
                              int(match.group('end_hour')),
                              int(match.group('end_min')),
                              int(match.group('end_sec')))

	#print("slc_end_dt : %s" %slc_end_dt)
        dates[h['_id']] = [ slc_start_dt, slc_end_dt ]
        footprints[h['_id']] = fields['location']
        metadata[h['_id']] = fields['metadata']
	#break
    #print("grouped : %s" %grouped)
    logger.info("grouped keys : %s" %grouped.keys())
    return {
        "hits": hits,
        "grouped": grouped,
        "dates": dates,
        "footprints": footprints,
        "metadata": metadata,
    }



def dataset_exists(id, index_suffix):
    """Query for existence of dataset by ID."""

    # es_url and es_index
    es_url = GRQ_URL
    es_index = "grq_*_{}".format(index_suffix.lower())
    
    # query
    query = {
        "query":{
            "bool":{
                "must":[
                    { "term":{ "_id": id } },
                ]
            }
        },
        "fields": [],
    }

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))
    if r.status_code == 200:
        result = r.json()
        total = result['hits']['total']
    else:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        if r.status_code == 404: total = 0
        else: r.raise_for_status()
    return False if total == 0 else True

def get_dataset(id, index_suffix):
    """Query for existence of dataset by ID."""

    # es_url and es_index
    es_url = GRQ_URL
    es_index = "grq_*_{}".format(index_suffix.lower())
    #es_index = "grq"

    # query
    query = {
        "query":{
            "bool":{
                "must":[
                    { "term":{ "_id": id } }
                ]
            }
        },
        "fields": []
    }

    print(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    print(result['hits']['total'])
    return result

def get_dataset(id):
    """Query for existence of dataset by ID."""

    # es_url and es_index
    es_url = GRQ_URL
    #es_index = "grq_*_{}".format(index_suffix.lower())
    es_index = "grq"

    # query
    query = {
        "query":{
            "bool":{
                "must":[
                    { "term":{ "_id": id } }
                ]
            }
        },
        "fields": []
    }

    print(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    print(result['hits']['total'])
    return result

def query_es2(query, es_index=None):
    """Query ES."""
    logger.info(query)
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


def query_es(endpoint, doc_id):
    """
    This function queries ES
    :param endpoint: the value specifies which ES endpoint to send query
     can be MOZART or GRQ
    :param doc_id: id of product or job
    :return: result from elasticsearch
    """
    es_url, es_index = None, None
    if endpoint == GRQ_ES_ENDPOINT:
        es_url = app.conf["GRQ_ES_URL"]
        es_index = "grq"
    if endpoint == MOZART_ES_ENDPOINT:
        es_url = app.conf['JOBS_ES_URL']
        es_index = "job_status-current"

    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"_id": doc_id}} # add job status:
                ]
            }
        }
    }

    #ES = elasticsearch.Elasticsearch(es_url)
    #result = ES.search(index=es_index, body=query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()

    if len(result["hits"]["hits"]) == 0:
        raise ValueError("Couldn't find record with ID: %s, at ES: %s"%(doc_id, es_url))
        return

    #LOGGER.debug("Got: {0}".format(json.dumps(result)))
    return result


def check_ES_status(doc_id):
    """
    There is a latency in the update of ES job status after
    celery signals job completion.
    To handle that case, we much poll ES (after sciflo returns status after blocking)
    until the job status is correctly reflected.
    :param doc_id: ID of the Job ES doc
    :return: True  if the ES has updated job status within 5 minutes
            otherwise raise a run time error
    """
    es_url = app.conf['JOBS_ES_URL']
    es_index = "job_status-current"
    query = {
        "_source": [
                   "status"
               ],
        "query": {
            "bool": {
                "must": [
                    {"term": {"_id": doc_id}}
                ]
            }
        }
    }

    #ES = elasticsearch.Elasticsearch(es_url)
    #result = ES.search(index=es_index, body=query)
    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()


    sleep_seconds = 2
    timeout_seconds = 300
    # poll ES until job status changes from "job-started" or for the job doc to show up. The poll will timeout soon after 5 mins.

    while len(result["hits"]["hits"]) == 0: #or str(result["hits"]["hits"][0]["_source"]["status"]) == "job-started":
        if sleep_seconds >= timeout_seconds:
            if len(result["hits"]["hits"]) == 0:
                raise RuntimeError("ES taking too long to index job with id %s."%doc_id)
            else:
                raise RuntimeError("ES taking too long to update status of job with id %s."%doc_id)
        time.sleep(sleep_seconds)
        #result = ES.search(index=es_index, body=query)

        r = requests.post(search_url, data=json.dumps(query))

        if r.status_code != 200:
            print("Failed to query %s:\n%s" % (es_url, r.text))
            print("query: %s" % json.dumps(query, indent=2))
            print("returned: %s" % r.text)
            r.raise_for_status()

        result = r.json()
        sleep_seconds = sleep_seconds * 2

    logging.info("Job status updated on ES to %s"%str(result["hits"]["hits"][0]["_source"]["status"]))
    return True

def get_complete_grq_data(id):
    es_url = GRQ_URL
    es_index = "grq"
    query = {
      "query": {
        "bool": {
          "must": [
            {
              "term": {
                "_id": id
              }
            }
          ]
        }
      }
    }


    print(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    print(result['hits']['total'])
    return result['hits']['hits']

def get_partial_grq_data(id):
    es_url = GRQ_URL
    es_index = "grq"

    query = {
        "query": {
            "term": {
                "_id": id,
            },
        },
        "partial_fields" : {
            "partial" : {
                "exclude" : "city",
            }
        }
    }

    print(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    print(result['hits']['total'])
    return result['hits']['hits'][0]

def get_acquisition_data(id):
    es_url = GRQ_URL
    es_index = "grq_*_*acquisition*"
    query = {
      "query": {
        "bool": {
          "must": [
            {
              "term": {
                "_id": id
              }
            }
          ]
        }
      },
      "partial_fields": {
        "partial": {
          "include": [
            "id",
            "dataset_type",
            "dataset",
            "metadata",
            "city",
            "continent"
          ]
        }
      }
    }


    print(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    print(result['hits']['total'])
    return result['hits']['hits']


def group_acqs_by_track(frames):
    grouped = {}
    acq_info = {}
    #print("frame length : %s" %len(frames))
    for acq in frames:
        #logger.info("ACQ : %s" %acq)
        acq_data = acq # acq['fields']['partial'][0]
        acq_id = acq['id']
        #print("acq_id : %s : %s" %(type(acq_id), acq_id))
        match = SLC_RE.search(acq_id)
        if not match:
            logger.info("No Match : %s" %acq_id)
            continue
        download_url = acq_data['metadata']['download_url']
        track = acq_data['metadata']['trackNumber']
        location = acq_data['metadata']['location']
        starttime = acq_data['starttime']
        endtime = acq_data['endtime']
        direction = acq_data['metadata']['direction']
        orbitnumber = acq_data['metadata']['orbitNumber']
        identifier = acq['metadata']['identifier']
        pv = None
        if "processing_version" in  acq_data['metadata']:
            pv = acq_data['metadata']['processing_version']
        this_acq = ACQ(acq_id, download_url, track, location, starttime, endtime, direction, orbitnumber, identifier, pv)
        acq_info[acq_id] = this_acq

        #logger.info("Adding %s : %s : %s : %s" %(track, orbitnumber, pv, acq_id))
        #logger.info(grouped)
        bisect.insort(grouped.setdefault(track, []), acq_id)
        '''
        if track in grouped.keys():
            if orbitnumber in grouped[track].keys():
                if pv in grouped[track][orbitnumber].keys():
                    grouped[track][orbitnumber][pv] = grouped[track][orbitnumber][pv].append(slave_acq)
                else:
                    slave_acqs = [slave_acq]
                    slave_pv = {}
                
                    grouped[track][orbitnumber] = 
        '''
    return {"grouped": grouped, "acq_info" : acq_info}


def getUpdatedTime(s, m):
    #date = dateutil.parser.parse(s, ignoretz=True)
    new_date = s + timedelta(minutes = m)
    return new_date

def getUpdatedTimeStr(s, m):
    date = dateutil.parser.parse(s, ignoretz=True)
    new_date = date + timedelta(minutes = m)
    return new_date

def is_overlap(geojson1, geojson2):
    '''returns True if there is any overlap between the two geojsons. The geojsons
    are just a list of coordinate tuples'''
    p3=0
    p1=Polygon(geojson1[0])
    p2=Polygon(geojson2[0])
    if p1.intersects(p2):
        p3 = p1.intersection(p2).area/p1.area
    return p1.intersects(p2), p3


def is_within(geojson1, geojson2):
    '''returns True if there is any overlap between the two geojsons. The geojsons
    are just a list of coordinate tuples'''
    p1=Polygon(geojson1[0])
    p2=Polygon(geojson2[0])
    return p1.within(p2)


def find_overlap_match(master_acq, slave_acqs):
    #logger.info("\n\nmaster info : %s : %s : %s :%s" %(master_acq.tracknumber, master_acq.orbitnumber, master_acq.pv, master_acq.acq_id))
    #logger.info("slave info : ")
    master_loc = master_acq.location["coordinates"]

    #logger.info("\n\nmaster_loc : %s" %master_loc)
    overlapped_matches = {}
    for slave in slave_acqs:
        logger.info("SLAVE : %s" %slave.location)
        slave_loc = slave.location["coordinates"]
        #logger.info("\n\nslave_loc : %s" %slave_loc)
        is_over, overlap = is_overlap(master_loc, slave_loc)
        logger.info("is_overlap : %s" %is_over)
        logger.info("overlap area : %s" %overlap)
        if is_over:
            overlapped_matches[slave.acq_id] = slave.location
            logger.info("Overlapped slave : %s" %slave.acq_id)

    return overlapped_matches

def ref_truncated(ref_scene, matched_footprints, covth=.99):
    """Return True if reference scene will be truncated."""

    # geometries are in lat/lon projection
    src_srs = osr.SpatialReference()
    src_srs.SetWellKnownGeogCS("WGS84")
    #src_srs.ImportFromEPSG(4326)

    # use projection with unit as meters
    tgt_srs = osr.SpatialReference()
    tgt_srs.ImportFromEPSG(3857)

    # create transformer
    transform = osr.CoordinateTransformation(src_srs, tgt_srs)
    
    # get polygon to fill if specified
    ref_geom = ogr.CreateGeometryFromJson(json.dumps(ref_scene.location))
    ref_geom_tr = ogr.CreateGeometryFromJson(json.dumps(ref_scene.location))
    ref_geom_tr.Transform(transform)
    ref_geom_tr_area = ref_geom_tr.GetArea() # in square meters
    logger.info("Reference GeoJSON: %s" % ref_geom.ExportToJson())

    # get union geometry of all matched scenes
    matched_geoms = []
    matched_union = None
    matched_geoms_tr = []
    matched_union_tr = None
    ids = sorted(matched_footprints.keys())
    #ids.sort()
    #logger.info("ids: %s" % len(ids))
    for id in ids:
        geom = ogr.CreateGeometryFromJson(json.dumps(matched_footprints[id]))
        geom_tr = ogr.CreateGeometryFromJson(json.dumps(matched_footprints[id]))
        geom_tr.Transform(transform)
        matched_geoms.append(geom)
        matched_geoms_tr.append(geom_tr)
        if matched_union is None:
            matched_union = geom
            matched_union_tr = geom_tr
        else:
            matched_union = matched_union.Union(geom)
            matched_union_tr = matched_union_tr.Union(geom_tr)
    matched_union_geojson =  json.loads(matched_union.ExportToJson())
    logger.info("Matched union GeoJSON: %s" % json.dumps(matched_union_geojson))
    
    # check matched_union disjointness
    if len(matched_union_geojson['coordinates']) > 1:
        logger.info("Matched union is a disjoint geometry.")
        return True
            
    # check that intersection of reference and stitched scenes passes coverage threshold
    ref_int = ref_geom.Intersection(matched_union)
    ref_int_tr = ref_geom_tr.Intersection(matched_union_tr)
    ref_int_tr_area = ref_int_tr.GetArea() # in square meters
    logger.info("Reference intersection GeoJSON: %s" % ref_int.ExportToJson())
    logger.info("area (m^2) for intersection: %s" % ref_int_tr_area)
    cov = ref_int_tr_area/ref_geom_tr_area
    logger.info("coverage: %s" % cov)
    if cov < covth:
        logger.info("Matched union doesn't cover at least %s%% of the reference footprint." % (covth*100.))
        return True
   
    return False
def get_union_geojson_acqs(acqs):
    geoms = []
    union = None
    for acq in acqs:
        geom = ogr.CreateGeometryFromJson(json.dumps(acq.location))
        geoms.append(geom)
        union = geom if union is None else union.Union(geom)
    union_geojson =  json.loads(union.ExportToJson())
    return union_geojson
    
def get_union_geometry(geojsons):
    """Return polygon of union of acquisition footprints."""

    # geometries are in lat/lon projection
    #src_srs = osr.SpatialReference()
    #src_srs.SetWellKnownGeogCS("WGS84")
    #src_srs.ImportFromEPSG(4326)

    # get union geometry of all scenes
    geoms = []
    union = None
    for geojson in geojsons:
        geom = ogr.CreateGeometryFromJson(json.dumps(geojson))
        geoms.append(geom)
        union = geom if union is None else union.Union(geom)
    union_geojson =  json.loads(union.ExportToJson())
    return union_geojson

def get_acq_orbit_polygon(starttime, endtime, orbit_dir):
    pass
    
def get_intersection(js1, js2):
    logger.info("intersection between :\n %s\n%s" %(js1, js2))
    poly1 = ogr.CreateGeometryFromJson(json.dumps(js1, indent=2, sort_keys=True))
    poly2 = ogr.CreateGeometryFromJson(json.dumps(js2, indent=2, sort_keys=True))

    intersection = poly1.Intersection(poly2)
    return json.loads(intersection.ExportToJson()), intersection.GetEnvelope()


def get_combined_polygon():
    pass

def get_time(t):
    try:
        return datetime.strptime(t, '%Y-%m-%dT%H:%M:%S')
    except ValueError as e:
        t1 = datetime.strptime(t, '%Y-%m-%dT%H:%M:%S.%f').strftime("%Y-%m-%d %H:%M:%S")
        return datetime.strptime(t1, '%Y-%m-%d %H:%M:%S')

def get_groundTrack_footprint(tstart, tend, orbit_file):
    mission = MISSION
    gt_footprint = []
    gt_footprint_temp= groundTrack.get_ground_track(tstart, tend, mission, orbit_file)
    for g in gt_footprint_temp:
        gt_footprint.append(list(g))

    gt_footprint.append(gt_footprint[0])

    #logger.info("gt_footprint : %s:" %gt_footprint)
    geojson = {"type":"Polygon", "coordinates": [gt_footprint]}
    return geojson

def get_area_from_orbit_file(tstart, tend, orbit_file, aoi_location):
    water_area = 0
    land_area = 0
    logger.info("tstart : %s  tend : %s" %(tstart, tend))
    geojson = get_groundTrack_footprint(tstart, tend, orbit_file)
    intersection, int_env = get_intersection(aoi_location, geojson)
    logger.info("intersection : %s" %intersection)
    land_area = lightweight_water_mask.get_land_area(intersection)
    logger.info("get_land_area(geojson) : %s " %land_area)
    water_area = lightweight_water_mask.get_water_area(intersection)

    logger.info("covers_land : %s " %lightweight_water_mask.covers_land(geojson))
    logger.info("covers_water : %s "%lightweight_water_mask.covers_water(geojson))
    logger.info("get_land_area(geojson) : %s " %land_area)
    logger.info("get_water_area(geojson) : %s " %water_area)    
    

    return land_area, water_area

def get_area_from_acq_location(geojson, aoi_location):
    logger.info("geojson : %s" %geojson)
    #geojson = {'type': 'Polygon', 'coordinates': [[[103.15855743232284, 69.51079998415891], [102.89429022592347, 69.19035954199457], [102.63670032476269, 68.86960457132169], [102.38549346807442, 68.5485482943004], [102.14039201693016, 68.22720313138305], [96.26595865368236, 68.7157534947759], [96.42758479823551, 69.0417647836668], [96.59286420765027, 69.36767025780232], [96.76197281310075, 69.69346586050469], [96.93509782364329, 70.019147225528]]]}
    intersection, int_env = get_intersection(aoi_location, geojson)
    logger.info("intersection : %s" %intersection)
    land_area = lightweight_water_mask.get_land_area(intersection)
    water_area = lightweight_water_mask.get_water_area(intersection)

    logger.info("covers_land : %s " %lightweight_water_mask.covers_land(geojson))
    logger.info("covers_water : %s "%lightweight_water_mask.covers_water(geojson))
    logger.info("get_land_area(geojson) : %s " %land_area)
    logger.info("get_water_area(geojson) : %s " %water_area)                                    
    

    return land_area, water_area



def get_processing_version(slc):
    pv = get_processing_version_from_scihub(slc)
    if not pv:
        pv = get_processing_version_from_asf(slc)
    return pv

def get_processing_version_from_scihub(slc):

    ipf_string = None

    return ipf_string

def get_processing_version_from_asf(slc):

    ipf = None

    try:
        # query the asf search api to find the download url for the .iso.xml file
        request_string = 'https://api.daac.asf.alaska.edu/services/search/param?platform=SA,SB&processingLevel=METADATA_SLC&granule_list=%s&output=json' %slc
        response = requests.get(request_string)

        response.raise_for_status()
        results = json.loads(response.text)

        # download the .iso.xml file, assumes earthdata login credentials are in your .netrc file
        response = requests.get(results[0][0]['downloadUrl'])
        response.raise_for_status()

        # parse the xml file to extract the ipf version string
        root = ElementTree.fromstring(response.text.encode('utf-8'))
        ns = {'gmd': 'http://www.isotc211.org/2005/gmd', 'gmi': 'http://www.isotc211.org/2005/gmi', 'gco': 'http://www.isotc211.org/2005/gco'}
        ipf_string = root.find('gmd:composedOf/gmd:DS_DataSet/gmd:has/gmi:MI_Metadata/gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:lineage/gmd:LI_Lineage/gmd:processStep/gmd:LI_ProcessStep/gmd:description/gco:CharacterString', ns).text

        if ipf_string:
            ipf = ipf_string.split('version')[1].split(')')[0].strip()
    except Exception as err:
        logger.info("get_processing_version_from_asf : %s" %str(err))
 
        
    return ipf

def print_acquisitions(aoi_id, acqs):
    logger.info("PRINT_ACQS")
    for acq in acqs:
        #aoi_id = acq_data['aoi_id']
        logger.info("aoi : %s track: %s orbitnumber : %s pv: %s acq_id : %s" %(aoi_id, acq.tracknumber, acq.orbitnumber, acq.pv, acq.acq_id))
    logger.info("\n")


def get_overlapping_slaves_query(master):
    return get_overlapping_slaves_query(master.starttime, master.endtime, master.location, master.tracknumber, master.direction, master.orbitnumber)
    
def get_overlapping_slaves_query(starttime, endtime, location, tracknumber, direction, orbitnumber):
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
                                      "shape": location
                                    }
                                }},
				{	
                                "range" : {
                                    "endtime" : {
                                        "lte" : starttime
                
                                    }
                                }},
				{ "term": { "trackNumber": tracknumber }},
				{ "term": { "direction": direction }}
			    ],
			"must_not": { "term": { "orbitNumber": orbitnumber }}
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

def get_overlapping_slaves_query_orbit(master, orbitnumber):
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
                                { "term": { "orbitNumber": orbitnumber }},
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


'''
def query_es(query, es_index):
    """Query ES."""

    es_url = GRQ_URL
    rest_url = es_url[:-1] if es_url.endswith('/') else es_url
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



def resolve_s1_slc(identifier, download_url, project):
    #Resolve S1 SLC using ASF datapool (ASF or NGAP). Fallback to ESA

    # determine best url and corresponding queue
    vertex_url = "https://datapool.asf.alaska.edu/SLC/SA/{}.zip".format(identifier)
    r = requests.head(vertex_url, allow_redirects=True)
    if r.status_code == 403:
        url = r.url
        queue = "{}-job_worker-small".format(project)
    elif r.status_code == 404:
        url = download_url
        queue = "factotum-job_worker-scihub_throttled"
    else:
        raise RuntimeError("Got status code {} from {}: {}".format(r.status_code, vertex_url, r.url))
    return url, queue


class DatasetExists(Exception):
    """Exception class for existing dataset."""
    pass



def resolve_source():
    """Resolve best URL from acquisition."""


    # get settings

    context_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), '_context.json')
    with open(context_file) as f:
        ctx = json.load(f)


    settings_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'settings.json')
    with open(settings_file) as f:
        settings = json.load(f)
    

    # build args
    spyddder_extract_versions = []
    standard_product_versions = []
    queues = []
    urls = []
    archive_filenames = []
    identifiers = []
    prod_dates = []
    priorities = []
    aois = []
    ds_exists = False



    # ensure acquisition
    if ctx['dataset_type'] != "acquisition":
        raise RuntimeError("Invalid dataset type: {}".format(ctx['dataset_type']))

    # route resolver and return url and queue
    if ctx['dataset'] == "acquisition-S1-IW_SLC":
        result = get_dataset(ctx['identifier'], settings['ACQ_TO_DSET_MAP'][ctx['dataset']])
        total = result['hits']['total']
        print("Total dataset found : %s" %total)

        if total > 0:
            #raise DatasetExists("Dataset {} already exists.".format(ctx['identifier']))
            print("dataset exists")
            ds_exists = True
        else:
            ds_exists = False
            url, queue = resolve_s1_slc(ctx['identifier'], ctx['download_url'], ctx['project'])
            queues.append(queue)
            urls.append(url)

        spyddder_extract_versions.append(ctx['spyddder_extract_version'])
        spyddder_extract_versions.append(ctx['spyddder_extract_version'])
        archive_filenames.append(ctx['archive_filename'])
        identifiers.append(ctx['identifier'])
        prod_dates.append(time.strftime('%Y-%m-%d' ))
        priorities.append( ctx.get('job_priority', 0))
        aois.append(ctx.get('aoi', 'no_aoi'))
            
    else:
        raise NotImplementedError("Unknown acquisition dataset: {}".format(ctx['dataset']))


    return ( ds_exists, spyddder_extract_versions, spyddder_extract_versions, queues, urls, archive_filenames,
             identifiers, prod_dates, priorities, aois )



def resolve_source_from_ctx_file(ctx_file):

    """Resolve best URL from acquisition."""

    with open(ctx_file) as f:
        return resolve_source(json.load(f))
'''


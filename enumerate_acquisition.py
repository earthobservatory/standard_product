import os, sys, re, requests, json, logging, traceback, argparse, copy, bisect
import util
from hysds.celery import app
import os, sys, re, requests, json, logging, traceback, argparse, copy, bisect
import hashlib
from itertools import product, chain
from datetime import datetime, timedelta
import numpy as np
from osgeo import ogr, osr
from pprint import pformat
from collections import OrderedDict
from shapely.geometry import Polygon

#import isce
#from UrlUtils import UrlUtils as UU

class ACQ:
    def __init__(self, acq_id, download_url, tracknumber, location, starttime, endtime, direction, orbitnumber, pv ):
	self.acq_id=acq_id,
	self.download_url = download_url
	self.tracknumber = tracknumber
        self.location= location
	self.starttime = starttime
	self.endtime = endtime
	self.pv = pv
	self.direction = direction
        self.orbitnumber = orbitnumber
        print("%s, %s, %s, %s, %s, %s, %s, %s, %s" %(acq_id, download_url, tracknumber, location, starttime, endtime, direction, orbitnumber, pv))




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



def run_acq_query(query):
    es_url = app.conf.GRQ_ES_URL
    es_index = "grq_*_*acquisition*"

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
    ids = matched_footprints.keys()
    ids.sort()
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
        slave_loc = slave.location["coordinates"]
        #logger.info("\n\nslave_loc : %s" %slave_loc)
        is_over, overlap = is_overlap(master_loc, slave_loc)
        logger.info("is_overlap : %s" %is_over)
        logger.info("overlap area : %s" %overlap)
        if is_over:
            overlapped_matches[slave.acq_id] = slave.location
            logger.info("Overlapped slave : %s" %slave.acq_id)

    return overlapped_matches

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

def group_acqs_by_orbitnumber(frames):
    grouped = {}
    acq_info = {}
    print("frame length : %s" %len(frames))
    for acq in frames:
	acq_data = acq['fields']['partial'][0]
	acq_id = acq['_id']
	match = SLC_RE.search(acq_id)
        if not match:
	    logger.info("No Match : %s" %acq_id)
	    continue
	download_url = acq_data['metadata']['download_url'] 
	track = acq_data['metadata']['trackNumber']
	location = acq_data['location']
	starttime = acq_data['starttime']
	endtime = acq_data['endtime']
        direction = acq_data['metadata']['direction']
        orbitnumber = acq_data['metadata']['orbitNumber']
        pv = acq_data['metadata']['processing_version']
	slave_acq = ACQ(acq_id, download_url, track, location, starttime, endtime, direction, orbitnumber, pv)
        acq_info[acq_id] = slave_acq
       
        logger.info("Adding %s : %s : %s : %s" %(track, orbitnumber, pv, acq_id))
	#logger.info(grouped)
        bisect.insort(grouped.setdefault(track, {}).setdefault(orbitnumber, {}).setdefault(pv, []), slave_acq.acq_id)
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
        print("h['_id'] : %s" %h['_id'])

        # get product url; prefer S3
        prod_url = fields['urls'][0]
        if len(fields['urls']) > 1:
            for u in fields['urls']:
                if u.startswith('s3://'):
                    prod_url = u
                    break
        print("prod_url : %s" %prod_url)
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
    print("grouped keys : %s" %grouped.keys())
    return {
        "hits": hits,
        "grouped": grouped,
        "dates": dates,
        "footprints": footprints,
        "metadata": metadata,
    }


def switch_references(master_acq, slaves):
    query = get_master_overlapped_query(master_acq)
 
    for slave in salves:
	query = get_overlapping_masters_query(master_acq, slave)
	find_match(slave, query, False, master)


def find_match(ref_acq, query, switch, must_acq=None):
    matched_acqs = process_query(query)
    

    grouped_matched = group_acqs_by_orbitnumber(matched_acqs)
    matched_ids = grouped_matched["acq_info"].keys()
    
    if must_acq is not None:
	logger.info(grouped_matched["grouped"])
	is must_acq.acq_id not in matched_ids:
	    logger.info("ERROR : master acq : %s not in matched acq list of the slave : "%must_acq.acq_id)
	else:
	    logger.info("ERROR : master acq : %s in matched acq list of the slave : "%must_acq.acq_id)
	
    
    #logger.info(grouped_slaves["acq_info"].keys())
    #logger.info(type(grouped_slaves["acq_info"]))
    #logger.info(grouped_slaves["grouped"])
    slc_count = 0
    pv_count = 0
    orbit_count =0
    track_count = 0
    for track in grouped_matched["grouped"]:
	track_count = track_count+1
	#logger.info("\n\n\nTRACK : %s" %track)
	for orbitnumber in grouped_matched["grouped"][track]:
 	    orbit_count= orbit_count+1
	    #logger.info("OrbitNumber : %s" %orbitnumber)
	    for pv in grouped_slaves["grouped"][track][orbitnumber]:
		#logger.info("\tpv : %s" %pv)
	 	pv_count = pv_count +1
                matched_acq_ids=grouped_matched["grouped"][track][orbitnumber][pv]
		matched_acqs = []
		for acq in matched_acq_ids:
		    slc_count=slc_count+1
		    #logger.info("]\t\t%s" %type(acq[0]))
		    if acq[0].strip() in grouped_matched["acq_info"].keys():
	            	acq_info =grouped_matched["acq_info"][acq[0].strip()]
		    	matched_acqs.append(acq_info) 
		    else:
			logger.info("Key does not exists" %acq[0].strip())   
		overlapped_matches = find_overlap_match(ref_acq, matched_acqs)
		if len(overlapped_matches)>0:
		    logger.info("Overlapped Acq exists for track: %s orbit_number: %s process version: %s. Now checking coverage." %(track, orbitnumber, pv))
		    union_loc = get_union_geometry(overlapped_matches)
		    logger.info("union loc : %s" %union_loc)

		    is_ref_truncated = ref_truncated(master_acq, overlapped_matches, covth=.99)
		    is_covered = is_within(master_acq.location["coordinates"], union_loc["coordinates"])
		    is_overlapped, overlap = is_overlap(master_acq.location["coordinates"], union_loc["coordinates"])
		    logger.info("is_ref_truncated : %s" %is_ref_truncated)
		    logger.info("is_within : %s" %is_covered)
		    logger.info("is_overlapped : %s, overlap : %s" %(is_overlapped, overlap))
        	    #logger.info("overlap area : %s" %overlap)
        	    if is_covered: # and overlap >=covth:
			logger.info("we have found a match :" )
		    else:
			logger.info("we have NOT found a match. So switching slaves...")
			slaves = []
			for slave_id in overlapped_matches.keys():
			    slaves.append(grouped_slaves["acq_info"][slave_id]
                        switch_references(master_acq, slaves)

		else:
		    logger.info("No Overlapped Acq for track: %s orbit_number: %s process version: %s" %(track, orbitnumber, pv))

def process_query(query):

    rest_url = app.conf.GRQ_ES_URL
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

def enumerate_acquisations_standard_product(acq_id):

    
    covth = 1.0

    # First lets find information about the acquisation
    acq = util.get_complete_acquisition_data(acq_id)[0]
    #print(acq)
    acq_data = acq['_source']
    print(acq_data['metadata']['download_url'])
    print(acq_data['starttime'])
    master_acq = ACQ(acq['_id'], acq_data['metadata']['download_url'], acq_data['metadata']['trackNumber'], acq_data['location'], acq_data['starttime'], acq_data['endtime'], acq_data['metadata']['direction'], acq_data['metadata']['orbitNumber'], acq_data['metadata']['processing_version'])
    master_scene = {
     'id': acq['_id'],
     'track': acq_data['metadata']['trackNumber'],
     'date': acq_data['starttime'],
     'location': acq_data['location'],
     'pre_matches': None,
     'post_matches': None 

    }
    #Now lets find all the acqusations that has same location but from previous date 
    ref_hits = []
    query = get_overlapping_slaves_query(master_acq)
    ref_hits = process_query(query)

    # extract reference ids
    #ref_ids = { h['_id']: True for h in ref_hits }
    #logger.info("ref_ids: {}".format(json.dumps(ref_ids, indent=2)))
    #logger.info("ref_hits count: {}".format(len(ref_hits)))



    grouped_slaves = group_acqs_by_orbitnumber(ref_hits)
    #logger.info(grouped_slaves["acq_info"].keys())
    #logger.info(type(grouped_slaves["acq_info"]))
    #logger.info(grouped_slaves["grouped"])
    slc_count = 0
    pv_count = 0
    orbit_count =0
    track_count = 0
    for track in grouped_slaves["grouped"]:
	track_count = track_count+1
	#logger.info("\n\n\nTRACK : %s" %track)
	for orbitnumber in grouped_slaves["grouped"][track]:
 	    orbit_count= orbit_count+1
	    #logger.info("OrbitNumber : %s" %orbitnumber)
	    for pv in grouped_slaves["grouped"][track][orbitnumber]:
		#logger.info("\tpv : %s" %pv)
	 	pv_count = pv_count +1
                slave_acq_ids=grouped_slaves["grouped"][track][orbitnumber][pv]
		slave_acqs = []
		for acq in slave_acq_ids:
		    slc_count=slc_count+1
		    #logger.info("]\t\t%s" %type(acq[0]))
		    if acq[0].strip() in grouped_slaves["acq_info"].keys():
	            	acq_info =grouped_slaves["acq_info"][acq[0].strip()]
		    	slave_acqs.append(acq_info) 
		    else:
			logger.info("Key does not exists" %acq.strp())   
		overlapped_matches = find_overlap_match(master_acq, slave_acqs)
		if len(overlapped_matches)>0:
		    logger.info("Overlapped Acq exists for track: %s orbit_number: %s process version: %s. Now checking coverage." %(track, orbitnumber, pv))
		    union_loc = get_union_geometry(overlapped_matches)
		    logger.info("union loc : %s" %union_loc)

		    is_ref_truncated = ref_truncated(master_acq, overlapped_matches, covth=.99)
		    is_covered = is_within(master_acq.location["coordinates"], union_loc["coordinates"])
		    is_overlapped, overlap = is_overlap(master_acq.location["coordinates"], union_loc["coordinates"])
		    logger.info("is_ref_truncated : %s" %is_ref_truncated)
		    logger.info("is_within : %s" %is_covered)
		    logger.info("is_overlapped : %s, overlap : %s" %(is_overlapped, overlap))
        	    #logger.info("overlap area : %s" %overlap)
        	    if is_covered: # and overlap >=covth:
			logger.info("we have found a match :" )
		    else:
			logger.info("we have NOT found a match. So switching slaves...")
			slaves = []
			for slave_id in overlapped_matches.keys():
			    slaves.append(grouped_slaves["acq_info"][slave_id]
                        switch_references(master_acq, slaves)

		else:
		    logger.info("No Overlapped Acq for track: %s orbit_number: %s process version: %s" %(track, orbitnumber, pv))
    logger.info("track_count : %s" %track_count)
    logger.info("orbit_count : %s" %orbit_count)
    logger.info("pv_count : %s" %pv_count)
    logger.info("slc_count : %s" %slc_count)

    


if __name__ == "__main__":
    acq_id = "acquisition-S1A_IW_SLC__1SDV_20180702T135953_20180702T140020_022616_027345_3578"
    enumerate_acquisations_standard_product(acq_id)

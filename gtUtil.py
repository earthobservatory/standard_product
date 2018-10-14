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
import util
from util import ACQ


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


def get_time(t):
    try:
        return datetime.strptime(t, '%Y-%m-%dT%H:%M:%S')
    except ValueError as e:
        t1 = datetime.strptime(t, '%Y-%m-%dT%H:%M:%S.%f').strftime("%Y-%m-%d %H:%M:%S")
        return datetime.strptime(t1, '%Y-%m-%d %H:%M:%S')

def get_area_from_orbit_file(tstart, tend, orbit_file, aoi_location):
    water_area = 0
    land_area = 0
    logger.info("tstart : %s  tend : %s" %(tstart, tend))
    geojson = get_groundTrack_footprint(tstart, tend, orbit_file)
    intersection, int_env = util.get_intersection(aoi_location, geojson)
    logger.info("intersection : %s" %intersection)
    land_area = lightweight_water_mask.get_land_area(intersection)
    logger.info("get_land_area(geojson) : %s " %land_area)
    water_area = lightweight_water_mask.get_water_area(intersection)

    logger.info("covers_land : %s " %lightweight_water_mask.covers_land(geojson))
    logger.info("covers_water : %s "%lightweight_water_mask.covers_water(geojson))
    logger.info("get_land_area(geojson) : %s " %land_area)
    logger.info("get_water_area(geojson) : %s " %water_area)    
    

    return land_area, water_area



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
        union_geojson = util.get_union_geometry(polygons)
        logger.info("union_geojson : %s" %union_geojson)
        #intersection, int_env = get_intersection(aoi['location'], union_geojson)
        #logger.info("union intersection : %s" %intersection)
        total_land, total_water = get_area_from_acq_location(union_geojson, aoi_location)
    


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

def get_area_from_acq_location(geojson, aoi_location):
    logger.info("geojson : %s" %geojson)
    #geojson = {'type': 'Polygon', 'coordinates': [[[103.15855743232284, 69.51079998415891], [102.89429022592347, 69.19035954199457], [102.63670032476269, 68.86960457132169], [102.38549346807442, 68.5485482943004], [102.14039201693016, 68.22720313138305], [96.26595865368236, 68.7157534947759], [96.42758479823551, 69.0417647836668], [96.59286420765027, 69.36767025780232], [96.76197281310075, 69.69346586050469], [96.93509782364329, 70.019147225528]]]}
    intersection, int_env = util.get_intersection(aoi_location, geojson)
    logger.info("intersection : %s" %intersection)
    land_area = lightweight_water_mask.get_land_area(intersection)
    water_area = lightweight_water_mask.get_water_area(intersection)

    logger.info("covers_land : %s " %lightweight_water_mask.covers_land(geojson))
    logger.info("covers_water : %s "%lightweight_water_mask.covers_water(geojson))
    logger.info("get_land_area(geojson) : %s " %land_area)
    logger.info("get_water_area(geojson) : %s " %water_area)


    return land_area, water_area


def getUpdatedTime(s, m):
    #date = dateutil.parser.parse(s, ignoretz=True)
    new_date = s + timedelta(minutes = m)
    return new_date




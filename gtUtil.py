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

def get_aoi_area_polygon(geojson, aoi_location):
    water_area = 0
    land_area = 0
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

def change_coordinate_direction(union_geom):
    logger.info("change_coordinate_direction")
    coordinates = union_geom["coordinates"]
    logger.info("Type of union polygon : %s of len %s" %(type(coordinates), len(coordinates)))
    for i in range(len(coordinates)):
        cord = coordinates[i]
        cord_area = util.get_area(cord)
        if not cord_area>0:
            logger.info("change_coordinate_direction : coordinates are not clockwise, reversing it")
            cord = [cord[::-1]]
            logger.info(cord)
            cord_area = util.get_area(cord)
            if not cord_area>0:
                logger.info("change_coordinate_direction. coordinates are STILL NOT  clockwise")
            union_geom["coordinates"][i] = cord
        else:
            logger.info("change_coordinate_direction: coordinates are already clockwise")

    return union_geom

def water_mask_test1(acq_info, grouped_matched_orbit_number,  aoi_location, orbit_file = None):

    passed = False
    starttimes = []
    endtimes = []
    polygons = []
    acqs_land = []
    acqs_water = []
    gt_polygons = []
    logger.info("water_mask_test1 : aoi_location : %s" %aoi_location)
    for pv in grouped_matched_orbit_number:
        acq_ids = grouped_matched_orbit_number[pv]
        for acq_id in acq_ids:
            logger.info("%s : %s" %(pv, acq_id))
            acq = acq_info[acq_id]
            starttimes.append(get_time(acq.starttime))
            endtimes.append(get_time(acq.endtime)) 
            polygons.append(acq.location)

            if orbit_file:
                gt_geojson = get_groundTrack_footprint(get_time(acq.starttime), get_time(acq.endtime), orbit_file)
                gt_polygons.append(gt_geojson)
              
    total_land = 0
    total_water = 0
    
    if orbit_file:
        union_gt_polygon = util.get_union_geometry(gt_polygons)
        union_gt_polygon = change_coordinate_direction(union_gt_polygon)
        logger.info("water_mask_test1 : union_gt_polygon : %s" %union_gt_polygon)
        #get lowest starttime minus 10 minutes as starttime
        tstart = getUpdatedTime(sorted(starttimes)[0], -10)
        logger.info("tstart : %s" %tstart)
        tend = getUpdatedTime(sorted(endtimes, reverse=True)[0], 10)
        logger.info("tend : %s" %tend)
        aoi_gt_geojson = get_groundTrack_footprint(tstart, tend, orbit_file)
        aoi_gt_polygon = change_coordinate_direction(aoi_gt_polygon)
        logger.info("water_mask_test1 : aoi_gt_geojson : %s" %aoi_gt_geojson)
        union_land, union_water = get_aoi_area_polygon(union_gt_polygon, aoi_location)
        logger.info("water_mask_test1 with Orbit File: union_land : %s union_water : %s" %(union_land, union_water))
        aoi_land, aoi_water = get_aoi_area_polygon(aoi_gt_geojson, aoi_location)
        logger.info("water_mask_test1 with Orbit File: aoi_land : %s aoi_water : %s" %(aoi_land, aoi_water))
        return isTrackSelected(union_land, aoi_land)
    else:        
        union_polygon = util.get_union_geometry(polygons)
        union_polygon = change_coordinate_direction(union_polygon)
        logger.info("Type of union polygon : %s of len %s" %(type(union_polygon["coordinates"]), len(union_polygon["coordinates"])))

        logger.info("water_mask_test1 without Orbit File : union_geojson : %s" %union_geojson)
        union_land, union_water = get_aoi_area_polygon(union_polygon, aoi_location)
        logger.info("water_mask_test1 without Orbit File: union_land : %s union_water : %s" %(union_land, union_water))
        aoi_land, aoi_water = get_aoi_area_polygon(aoi_location, aoi_location)
        logger.info("water_mask_test1 without Orbit File: aoi_land : %s aoi_water : %s" %(aoi_land, aoi_water))

        return isTrackSelected(union_land, aoi_land)


def isTrackSelected(union_land, aoi_land):
    selected = False
    logger.info("Area of union of acquisition land = %s" %union_land)
    logger.info("Area of AOI land = %s" %aoi_land)
    delta = abs(union_land - aoi_land)
    pctDelta = delta/union_land
    logger.info("delta : %s and pctDelta : %s" %(delta, pctDelta))
    if pctDelta <.02:
        logger.info("Track is SELECTED !!")
        return True
    logger.info("Track is NOT SELECTED !!")
    return False

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




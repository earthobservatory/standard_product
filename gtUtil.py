#!/usr/bin/env python 
import os, sys, time, json, requests, logging
import re, traceback, argparse, copy, bisect
from xml.etree import ElementTree
#from hysds_commons.job_utils import resolve_hysds_job
#from hysds.celery import app
from shapely.geometry import Polygon
from shapely.ops import cascaded_union
import datetime
from dateutil import parser
from datetime import datetime, timedelta
import groundTrack
from osgeo import ogr, osr
import lightweight_water_mask
import util
from math import sqrt
from util import ACQ, InvalidOrbitException, NoIntersectException
import urllib.request

#logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
#logger.setLevel(logging.INFO)

# set logger and custom filter to handle being run from sciflo
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger('gtUtil')
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())



#logger.addFilter(LogFilter())

SLC_RE = re.compile(r'(?P<mission>S1\w)_IW_SLC__.*?' +
                    r'_(?P<start_year>\d{4})(?P<start_month>\d{2})(?P<start_day>\d{2})' +
                    r'T(?P<start_hour>\d{2})(?P<start_min>\d{2})(?P<start_sec>\d{2})' +
                    r'_(?P<end_year>\d{4})(?P<end_month>\d{2})(?P<end_day>\d{2})' +
                    r'T(?P<end_hour>\d{2})(?P<end_min>\d{2})(?P<end_sec>\d{2})_.*$')

BASE_PATH = os.path.dirname(__file__)
MISSION = 'S1A'


def download_orbit_file(url, file_name):
    downloaded = False
    try:
        urllib.request.urlretrieve(url, file_name)
        downloaded = True
    except Exception as err:
        logger.debug("Error Downloading Orbit File : %s" %url)
        logger.debug(sys.exc_info())
    return downloaded


def get_groundTrack_footprint(tstart, tend, mission, orbit_file, orbit_dir):
    #mission = MISSION
    gt_footprint = []
    gt_footprint_temp= groundTrack.get_ground_track(tstart, tend, mission, orbit_file, orbit_dir)
    for g in gt_footprint_temp:
        gt_footprint.append(list(g))

    gt_footprint.append(gt_footprint[0])

    #logger.debug("gt_footprint : %s:" %gt_footprint)
    geojson = {"type":"Polygon", "coordinates": [gt_footprint]}
    return geojson

def water_mask_check(track, orbit_or_track_dt, acq_info, grouped_matched_orbit_number,  aoi_location, aoi_id, threshold_pixel, mission, orbit_type, orbit_file=None, orbit_dir=None):

    passed = False
    result = util.get_result_dict(aoi_id, track, orbit_or_track_dt)
    if not aoi_location:
        err_msg = "water_mask_check FAILED as aoi_location NOT found"
        result['fail_reason'] = err_msg
        logger.debug("err_msg")
        return False, {}, []
    try:
        passed, result, removed_ids = water_mask_test1(result, track, orbit_or_track_dt, acq_info, grouped_matched_orbit_number,  aoi_location, aoi_id, threshold_pixel, mission, orbit_type, orbit_file, orbit_dir)
        return passed, result, removed_ids
    except InvalidOrbitException as err:
        raise
    except Exception as err:
        err_msg = "orbit quality test failed : %s" %str(err)
        logger.debug(err_msg)
        result['fail_reason'] = err_msg
        traceback.print_exc()
        return False, result, []                                                                                                                                


def get_time(t):
     
    logger.debug("get_time(t) : %s" %t)
    t = parser.parse(t).strftime('%Y-%m-%dT%H:%M:%S')
    t1 = datetime.strptime(t, '%Y-%m-%dT%H:%M:%S')
    logger.debug("get_time : returning : %s" %t1)
    return t1


def get_time2(t):
    logger.debug("get_time(t) : %s" %t)
    t = t.upper().strip().split('.')[0].strip().split('Z')[0].strip()
    t1 = datetime.strptime(t, '%Y-%m-%dT%H:%M:%S')
    logger.debug("get_time2 : returning : %s" %t1)
    return t1


def get_area_from_orbit_file(tstart, tend, mision, orbit_file, orbit_dir, aoi_location):
    water_area = 0
    land_area = 0
    logger.info("get_area_from_orbit_file : tstart : %s  tend : %s" %(tstart, tend))
    geojson = get_groundTrack_footprint(tstart, tend, mission, orbit_file, orbit_dir)
    logger.info("get_area_from_orbit_file : geojson : %s" %geojson)
    land_area = 0
    water_area = 0
    intersection, int_env = util.get_intersection(aoi_location, geojson)
    logger.info("intersection of AOI and geojson: %s" %intersection)
    polygon_type = intersection['type']
    logger.info("intersection polygon_type : %s" %polygon_type)
    if polygon_type == "MultiPolygon":
        logger.debug("\n\nMULTIPOLIGON\n\n")
    land_area = lightweight_water_mask.get_land_area(intersection)
    logger.debug("get_land_area(geojson) : %s " %land_area)
    water_area = lightweight_water_mask.get_water_area(intersection)

    logger.debug("get_area_from_orbit_file : covers_land : %s " %lightweight_water_mask.covers_land(geojson))
    logger.debug("get_area_from_orbit_file: covers_water : %s "%lightweight_water_mask.covers_water(geojson))
    logger.debug("get_area_from_orbit_file : get_land_area(geojson) : %s " %land_area)
    logger.debug("get_area_from_orbit_file: get_water_area(geojson) : %s " %water_area)    
    

    return land_area, water_area

def get_aoi_area_multipolygon(geojson, aoi_location):
    water_area = 0
    land_area = 0

    polygon_type = geojson["type"]
    logger.debug("polygon_type : %s" %polygon_type)

    if polygon_type == "MultiPolygon":
        logger.info("MultiPolygon")
        coordinates = geojson["coordinates"]
        logger.debug("get_aoi_area_multipolygon : coordinate : %s" %coordinates)
        union_land = 0
        union_water = 0
        union_intersection = []
        for i in range(len(coordinates)):
            cord = coordinates[i]
            logger.debug("initial cord : %s " %cord)
            logger.debug("sending cord : %s" %cord[0])
            cord =change_coordinate_direction(cord[0])
            logger.debug("returning cord : %s " %cord)
         
            geojson_new = {"type":"Polygon", "coordinates": [cord]}
            logger.debug("get_aoi_area_multipolygon : geojson_new : %s" %geojson_new)
            land, water, intersection = get_aoi_area_polygon(geojson_new, aoi_location)
            logger.debug("land = %s, water = %s" %(land, water))
            union_land += land
            union_water += water
            if intersection:
                union_intersection.append(intersection)
        return union_land, union_water, union_intersection

    else:
        return get_aoi_area_polygon(geojson, aoi_location)

def get_aoi_area_polygon(geojson, aoi_location):
    water_area = 0
    land_area = 0
    
    logger.debug("\nget_aoi_area_polygon : \ngeojson : %s, \naoi_location : %s" %(geojson, aoi_location))
    intersection, int_env = util.get_intersection(aoi_location, geojson)
    logger.debug("intersection : %s" %intersection)
    polygon_type = intersection['type']
    logger.debug("intersection polygon_type : %s" %polygon_type)

    if polygon_type == "MultiPolygon":
        logger.debug("\n\nMULTIPOLIGON\n\n")
        return get_aoi_area_multipolygon(intersection, aoi_location)
    if "coordinates" in intersection:
        coordinates = intersection["coordinates"]
        cord =change_coordinate_direction(coordinates[0])
        intersection = {"type":"Polygon", "coordinates": [cord]}
        logger.debug("get_aoi_area_polygon : cord : %s" %cord)
    try:
        land_area = lightweight_water_mask.get_land_area(intersection)
        logger.debug("get_land_area(geojson) : %s " %land_area)
    except Exception as err:
        logger.debug("Getting Land Area Failed for geojson : %s" %intersection)
        cord = intersection["coordinates"][0]
        rotated_cord = [cord[::-1]]
        rotated_intersection = {"type":"Polygon", "coordinates": rotated_cord}
        logger.debug("rorated_intersection : %s" %rotated_intersection)
        
        land_area = lightweight_water_mask.get_land_area(rotated_intersection)
        logger.debug("get_land_area(geojson) : %s " %land_area)
    logger.debug("get_land_area(geojson) : %s " %land_area)
    logger.debug("get_water_area(geojson) : %s " %water_area)


    return land_area, water_area, intersection



def change_coordinate_direction(cord):
    logger.debug("change_coordinate_direction 1 cord: %s\n" %cord)
    cord_area = util.get_area(cord)
    if not cord_area>0:
        logger.debug("change_coordinate_direction : coordinates are not clockwise, reversing it")
        cord = [cord[::-1]]
        logger.debug("change_coordinate_direction 2 : cord : %s" %cord)
        try:
            cord_area = util.get_area(cord)
        except:
            cord = cord[0]
            logger.debug("change_coordinate_direction 3 : cord : %s" %cord)
            cord_area = util.get_area(cord)
        if not cord_area>0:
            logger.debug("change_coordinate_direction. coordinates are STILL NOT  clockwise")
    else:
        logger.debug("change_coordinate_direction: coordinates are already clockwise")

    logger.debug("change_coordinate_direction 4 : cord : %s" %cord)
    return cord


def change_union_coordinate_direction(union_geom):
    logger.debug("change_coordinate_direction")
    coordinates = union_geom["coordinates"]
    logger.debug("Type of union polygon : %s of len %s" %(type(coordinates), len(coordinates)))
    for i in range(len(coordinates)):
        cord = coordinates[i]
        cord_area = util.get_area(cord)
        if not cord_area>0:
            logger.debug("change_coordinate_direction : coordinates are not clockwise, reversing it")
            cord = [cord[::-1]]
            logger.debug(cord)
            cord_area = util.get_area(cord)
            if not cord_area>0:
                logger.debug("change_coordinate_direction. coordinates are STILL NOT  clockwise")
            union_geom["coordinates"][i] = cord
        else:
            logger.debug("change_coordinate_direction: coordinates are already clockwise")

    return union_geom



def get_acq_time_data(acq_info, acq_ids):
    starttimes = []
    endtimes = []
    for acq_id in acq_ids:
        logger.debug("\nACQ_ID%s : " %acq_id)
        acq = acq_info[acq_id]
        
        starttimes.append(get_time(acq.starttime))
        endtimes.append(get_time(acq.endtime))

        logger.debug("ACQ start time : %s " %acq.starttime)
        logger.debug("ACQ end time : %s" %acq.endtime)

    logger.debug("MIN start time : %s" %sorted(starttimes)[0])
    logger.debug("MAX end time : %s" %sorted(endtimes, reverse=True)[0])

    tstart = getUpdatedTime(sorted(starttimes)[0], -5)
    logger.debug("tstart : %s" %tstart)
    tend = getUpdatedTime(sorted(endtimes, reverse=True)[0], 5)
    logger.debug("tend : %s" %tend)
    logger.debug("\n\n\n\n")

def water_mask_test1(result, track, orbit_or_track_dt, acq_info, acq_ids,  aoi_location, aoi_id,  threshold_pixel, mission, orbit_type, orbit_file = None, orbit_dir = None):

    logger.info("\n\n\nWATER MASK TEST for Date : %s, track : %s aoi : %s orbit_file : %s aoi_location : %s and acqs : %s\n" %(orbit_or_track_dt, track, aoi_id, orbit_file, aoi_location, acq_ids ))
    #return True

    passed = False
    starttimes = []
    endtimes = []
    polygons = []
    acqs_land = []
    acqs_water = []
    gt_polygons = []
    removed_ids = []
    selected_ids = []
    #result['aoi'] = aoi_id
    #result['track'] = track
    #result['dt']  = orbit_or_track_dt
    '''
    if orbit_type=='P':
        result['primary_track_dt'] = orbit_or_track_dt
    else:
        result['secondary_track_dt'] = orbit_or_track_dt
    '''
    v = sqrt((2874.997595**2) +(4621.900513**2)+(-5292.166241**2))/1000.0
    logger.info("velocity of the satelite : %s" %v)

    acq_area_array = []
    gt_area_array = []

    aoi_info = util.get_dataset(aoi_id, "grq_*_area_of_interest")['hits']['hits'][0]['_source']
    logger.info(json.dumps(aoi_info))

    aoi_start_time = parser.parse(aoi_info['starttime']).replace(tzinfo=None)
    aoi_end_time = parser.parse(aoi_info['endtime']).replace(tzinfo=None)
    aoi_location_data = aoi_info['location']

    logger.info('aoi_start_time : {}'.format(aoi_start_time))
    logger.info('aoi_end_time : {}'.format(aoi_end_time))
    logger.info('aoi_location_data : {}'.format(aoi_location_data))
    logger.info('aoi_location : {}'.format(aoi_location))
    
    aoi_location_data_area= lightweight_water_mask.get_land_area(aoi_location_data)
    aoi_location_area= lightweight_water_mask.get_land_area(aoi_location)
    logger.info("aoi_location_data_area : %s" %aoi_location_data_area)
    logger.info("aoi_location_area : %s" %aoi_location_area)

    aoi_min_lat, aoi_max_lat = util.get_minmax(aoi_location)
    



    for acq_id in acq_ids:
        logger.info("\n\nProcessing Acq : %s : " %acq_id)
        acq = acq_info[acq_id]
        if acq.covers_only_land:
            logger.info("COVERS ONLY LAND")
        elif acq.covers_only_water:
            logger.info("COVERS ONLY WATER, SO RETURNING FALSE : %s" %acq_id)
            continue
            #return False, result
        else:
            logger.info("COVERS BOTH LAND & WATER")


        logger.debug("ACQ start time : %s " %acq.starttime)
        logger.debug("ACQ end time : %s" %acq.endtime)
        if parser.parse(acq.starttime)>= parser.parse(acq.endtime):
            err_msg = "ERROR : %s start time %s is greater or equal to its endtime %s" %(acq_id, acq.starttime, acq.endtime)
            result['fail_reason'] = err_msg
            logger.info(err_msg)
            return False, result, removed_ids
        else:
            logger.debug("Time check Passed")
        
        land = None 
        water = None
        acq_intersection=None
        try:
            land, water, acq_intersection = get_aoi_area_multipolygon(acq.location, aoi_location)
            acq_area_array.append(land)
        except Exception as err:
            err_msg = "Failed to get area of polygon : %s" %str(err)
            logger.info(err_msg)
            result['fail_reason'] = err_msg
            traceback.print_exc()
            #return False, result, removed_ids
        logger.info("Area from acq.location : %s" %land)

        if orbit_file:
            logger.debug("\n\nisValidOrbitTest for acquisition : %s\n" %acq_id)
            isValidOrbit = groundTrack.isValidOrbit(get_time(acq.starttime), get_time(acq.endtime), mission, orbit_file, orbit_dir)
            logger.info("gtUtil : isValidOrbit : %s" %isValidOrbit)
            if not isValidOrbit:
                err_msg = "Degraded Orbit : %s" %orbit_file
                logger.info(err_msg)
                result['fail_reason'] = err_msg
                #raise InvalidOrbitException(err_msg)
                return False, result, removed_ids
            gt_geojson = get_groundTrack_footprint(get_time(acq.starttime), get_time(acq.endtime), mission, orbit_file, orbit_dir)
            logger.info("gt_geojson of acq %s : %s" %(acq_id, gt_geojson))
            land = None 
            water = None
            acq_intersection=None
            try:
                land, water, acq_intersection= get_aoi_area_multipolygon(gt_geojson, aoi_location)

            except NoIntersectException as err:
                logger.info("\n\nError Calculating the intersection with AOI  of acquisition : %s" %acq_id)
                logger.info("Error : %s" %str(err))
                logger.info(traceback.format_exc())
                logger.info("\n Removing acquisition from list : %s" %acq_id)
                #acq_ids.remove(acq_id)
                removed_ids.append(acq_id)
                #drop this acqusition
                continue

                
            #As it is a valid and intersected acquisition, we can add its info in the calculation    
            logger.debug("\n\nValid Acquisition %s" %acq_id)
            logger.info("Area from gt_geojson : %s" %land)
            gt_area_array.append(land)
           
            logger.debug("Area from acq.location : %s" %land)
            polygons.append(acq.location)
            gt_polygons.append(gt_geojson)
            starttimes.append(get_time(acq.starttime))
            endtimes.append(get_time(acq.endtime))
            selected_ids.append(acq_id)
        else:
            raise RuntimeError("No Orbit File Found")

    logger.info("Sum of acq.location area : %s" %sum(acq_area_array))
    logger.info("Sum of gt location area : %s" %sum(gt_area_array))
    logger.info("selected_acqusitions : %s" %selected_ids)

    total_land = 0
    total_water = 0
   
    logger.debug("Calculating Union")
    union_polygon = util.get_union_geometry(polygons)
    union_polygon_min_lat, union_polygon_max_lat = util.get_minmax(union_polygon)
    logger.info("union_polygon_min_lat : %s, union_polygon_max_lat : %s" %(union_polygon_min_lat, union_polygon_max_lat))
    logger.info("aoi_min_lat : %s, aoi_max_lat : %s" %(aoi_min_lat, aoi_max_lat))

    max_lat_diff = 0
    min_lat_diff = 0
    if aoi_max_lat > union_polygon_max_lat:
        max_lat_diff = abs(aoi_max_lat - union_polygon_max_lat)*111
    if aoi_min_lat<union_polygon_min_lat:
        min_lat_diff = abs(union_polygon_min_lat - aoi_min_lat)*111

    logger.info("min_lat_diff : %s" %min_lat_diff)
    logger.info("max_lat_diff : %s" %max_lat_diff)
 
    start_time_pad = min_lat_diff/v
    end_time_pad = max_lat_diff/v

    logger.info("start_time_pad : %s" %start_time_pad)
    logger.info("end_time_pad : %s" %end_time_pad)

    if orbit_file:
        ''' First Try Without Orbit File 
        union_polygon = util.get_union_geometry(polygons)
        #union_polygon = change_coordinate_direction(union_polygon)
        logger.debug("Type of union polygon : %s of len %s" %(type(union_polygon["coordinates"]), len(union_polygon["coordinates"])))

        logger.debug("water_mask_test1 without Orbit File")
        union_land_no_orbit, union_water_no_orbit, union_intersection_no_orbit  = get_aoi_area_multipolygon(union_polygon, aoi_location)
        logger.debug("RESULT : AOI : %s, Track : %s, Date :  %s, Union_Acq_AOI, union_land : %s, union_water : %s, intersection : %s" %(aoi_id, track, orbit_or_track_dt, union_land_no_orbit, union_water_no_orbit, union_intersection_no_orbit))


        result['acq_union_land_area'] = union_land_no_orbit
        result['acq_union_aoi_intersection'] = union_intersection_no_orbit
        '''

        ''' Now Try With Orbit File '''
        logger.debug("water_mask_test1 with Orbit File")
        union_gt_polygon = util.get_union_geometry(gt_polygons)
        logger.info("union_gt_geojson : %s" %union_gt_polygon)
        union_land, union_water, union_intersection = get_aoi_area_multipolygon(union_gt_polygon, aoi_location)
        logger.info("water_mask_test1 with Orbit File: union_land : %s union_water : %s union intersection : %s" %(union_land, union_water, union_intersection))
        result['ACQ_POEORB_AOI_Intersection'] = union_intersection
        result['ACQ_Union_POEORB_Land'] = union_land
        '''
        if orbit_type == 'P':
            result['ACQ_POEORB_AOI_Intersection_primary'] = union_intersection
            result['ACQ_Union_POEORB_Land_primary'] = union_land
        else:
            result['ACQ_POEORB_AOI_Intersection_secondary'] = union_intersection
            result['ACQ_Union_POEORB_Land_secondary'] = union_land
        '''
        #get lowest starttime minus 10 minutes as starttime


        tstart = sorted(starttimes)[0].replace(tzinfo=None)
        '''
        if aoi_start_time<tstart:
            logger.info("resetting starttime from {} to {}".format(tstart, aoi_start_time))
            tstart = aoi_start_time
        '''

        tstart = getUpdatedTime(tstart, -(start_time_pad+5))
        logger.debug("tstart : %s" %tstart)
        
        tend = sorted(endtimes, reverse=True)[0].replace(tzinfo=None)

        '''
        if aoi_end_time>tend:
            logger.info("resetting endtime from {} to {}".format(tend, aoi_end_time))
            tend = aoi_end_time
        '''
        tend = getUpdatedTime(tend, (end_time_pad+5))
        logger.debug("tend : %s" %tend)
        

        track_gt_geojson = get_groundTrack_footprint(tstart, tend, mission, orbit_file, orbit_dir)
        logger.info("with  tstart : %s, tend : %s, track_gt_geojson : %s" %(tstart, tend, track_gt_geojson))
        track_land, track_water, track_intersection = get_aoi_area_multipolygon(track_gt_geojson, aoi_location)
        logger.info("water_mask_test1 with Orbit File: track_land : %s track_water : %s intersection : %s" %(track_land, track_water, track_intersection))
        result['Track_POEORB_Land'] = track_land
        result['Track_AOI_Intersection'] = track_intersection
        '''
        if orbit_type == 'P':
            result['Track_POEORB_Land_primary'] = union_intersection
            result['Track_AOI_Intersection_primary'] = union_land
        else:
            result['Track_POEORB_Land_secondary'] = union_intersection
            result['Track_AOI_Intersection_secondary'] = union_land
        '''
        is_selected, result = isTrackSelected(selected_ids, track, orbit_or_track_dt, union_land, union_water, track_land, track_water, aoi_id, threshold_pixel, union_intersection, track_intersection, result)
        return is_selected, result, removed_ids
    else:
        err_msg = "No Orbit file"
        result['fail_reason'] = err_msg  
        logger.debug("\n\nNO ORBIT\n\n")
        return False, result, removed_ids
      
        '''
        union_polygon = util.get_union_geometry(polygons)
        union_polygon = change_coordinate_direction(union_polygon)
        logger.debug("Type of union polygon : %s of len %s" %(type(union_polygon["coordinates"]), len(union_polygon["coordinates"])))

        logger.debug("water_mask_test1 without Orbit File : union_geojson : %s" %union_geojson)
        union_land, union_water = get_aoi_area_multipolygon(union_polygon, aoi_location)
        logger.debug("water_mask_test1 without Orbit File: union_land : %s union_water : %s" %(union_land, union_water))
        track_land, track_water = get_aoi_area_multipolygon(aoi_location, aoi_location)
        logger.debug("water_mask_test1 without Orbit File: track_land : %s track_water : %s" %(track_land, track_water))

        return isTrackSelected(union_land, track_land)
        '''


def isTrackSelected(selected_ids, track, orbit_or_track_dt, union_land, union_water, track_land, track_water, aoi_id, threshold_pixel, union_intersection, track_intersection, result):
    selected = False

    logger.info("\nFinal Result:")
    logger.info("AOI : %s, Track : %s, Date :  %s" %(aoi_id, track, orbit_or_track_dt))
    logger.info("selected acqs : %s" %selected_ids)
    logger.info("acq union_land : %s, acq union_water : %s" %(union_land, union_water))
    logger.info("track land : %s  track_water : %s" %(track_land, track_water))
    logger.debug("union_intersection : %s" %union_intersection)
    logger.debug("track_intersection : %s" %track_intersection)
    #logger.debug("RESULT : AOI : %s, Track : %s, Date :  %s, Union_POEORB_Acq_AOI, union_land : %s, union_water : %s, intersection : %s" %(aoi_id, track, orbit_or_track_dt, union_land, union_water, union_intersection))
    #logger.debug("RESULT : AOI : %s, Track : %s, Date :  %s, POEORB_Track_AOI, union_land : %s, union_water : %s, intersection : %s" %(aoi_id, track, orbit_or_track_dt, track_land, track_water, track_intersection))

    #logger.debug("RESULT : AOI : %s Track : %s Date : %s : Area of AOI land = %s" %(aoi_id, track, orbit_or_track_dt, track_land))
    if union_land == 0 or track_land == 0:
        err_msg = "Land aria calculation is not correct. track land area = %s. Union of acqusition land area = %s" %(track_land, union_land)
        result['fail_reason'] = err_msg
        logger.info("\nERROR : isTrackSelected : Returning as lands are Not correct")
        return False, result
    delta_A = abs(float(union_land - track_land))
    pctDelta = float(delta_A/track_land)
    delta_x = float(delta_A/250)
    logger.info("union and track land are diff : %s sqm, dividing that by 250 : %s and delta percentage : %s" %(delta_A, delta_x, pctDelta))
    result['delta_area'] = delta_A 
    # Assiuming 90 m resolution, lets change it to km
    res_km = float(90/1000)

    res = delta_x/res_km

    logger.debug("delta resolution : (delta area*1000)/(90 *250) : %s and  threshold_value : %s" %(res, threshold_pixel))    

    result['res'] = res
    #if pctDelta <.1:
    if res <threshold_pixel:
        result['area_threshold_passed']= True
        logger.info("Track is SELECTED !!: %s" %orbit_or_track_dt)
        result['WATER_MASK_PASSED'] = True
        return True, result
    else:
        err_msg = "Acqusition Land Coverage is lower than required by track. Possibly missing scene"
        logger.info(err_msg)
        result['fail_reason'] = err_msg
        result['area_threshold_passed']=False
        logger.debug("Track is NOT SELECTED !! : %s" %orbit_or_track_dt)
        return False, result

def get_area_from_acq_location(geojson, aoi_location):
    logger.debug("geojson : %s" %geojson)
    land_area = 0
    water_area = 0
    
    intersection, int_env = util.get_intersection(aoi_location, geojson)
    logger.debug("intersection : %s" %intersection)
    polygon_type = intersection['type']
    logger.debug("get_area_from_acq_location : intersection polygon_type : %s" %polygon_type)

    if polygon_type == "MultiPolygon":
        logger.debug("\n\nMULTIPOLIGON\n\n")

    land_area = lightweight_water_mask.get_land_area(intersection)
    water_area = lightweight_water_mask.get_water_area(intersection)

    logger.debug("covers_land : %s " %lightweight_water_mask.covers_land(geojson))
    logger.debug("covers_water : %s "%lightweight_water_mask.covers_water(geojson))
    logger.debug("get_land_area(geojson) : %s " %land_area)
    logger.debug("get_water_area(geojson) : %s " %water_area)


    return land_area, water_area


def getUpdatedTime(s, m):
    #date = dateutil.parser.parse(s, ignoretz=True)
    new_date = s + timedelta(minutes = m)
    return new_date




#!/usr/bin/env python3 
import os, sys, time, json, requests, logging
import re, traceback, argparse, copy, bisect
from xml.etree import ElementTree
#from hysds_commons.job_utils import resolve_hysds_job
#from hysds.celery import app
import util
from util import ACQ
import datetime  
import dateutil.parser
from datetime import datetime, timedelta
import groundTrack
from osgeo import ogr
import lightweight_water_mask


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

SLC_RE = re.compile(r'(?P<mission>S1\w)_IW_SLC__.*?' +
                    r'_(?P<start_year>\d{4})(?P<start_month>\d{2})(?P<start_day>\d{2})' +
                    r'T(?P<start_hour>\d{2})(?P<start_min>\d{2})(?P<start_sec>\d{2})' +
                    r'_(?P<end_year>\d{4})(?P<end_month>\d{2})(?P<end_day>\d{2})' +
                    r'T(?P<end_hour>\d{2})(?P<end_min>\d{2})(?P<end_sec>\d{2})_.*$')

BASE_PATH = os.path.dirname(__file__)
MISSION = 'S1A'


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




def query_aois(starttime, endtime):
    """Query ES for active AOIs that intersect starttime and endtime."""

    es_index = "grq_*_area_of_interest"
    query = {
        "query": {
            "bool": {
                "should": [
                    {
                        "bool": {
                            "must": [
                                {
                                    "range": {
                                        "starttime": {
                                            "lte": endtime
                                        }
                                    }
                                },
                                {
                                    "range": {
                                        "endtime": {
                                            "gte": starttime
                                        }
                                    }
                                },
				{
                                    "match": {
					    "dataset_type": "area_of_interest"
                                        }
                                }
                            ]
                        }
                    },
        {
        "filtered": {
            "query": {

              "bool": {
                "must": [
                  {
                    "match": {
                      "dataset_type": "area_of_interest"
                      }
                  },
                  {
                    "range": {
                      "starttime": {
                        "lte": endtime
                      }
                    }
                  }
                ]
              }
            },
            "filter": {
              "missing": {
                "field": "endtime"
              }
            }
          }
        },
        {
        "filtered": {
            "query": {

              "bool": {
                "must": [
                  {
                    "match": {
                      "dataset_type": "area_of_interest"
                      }
                  },
                  {
                    "range": {
                      "endtime": {
                        "gte": starttime
                      }
                    }
                  }
                ]
              }
            },
            "filter": {
              "missing": {
                "field": "starttime"
              }
            }
          }
        }
          
      ]
    }
  },
        "partial_fields" : {
            "partial" : {
                "include" : [ "id", "starttime", "endtime", "location", 
                              "metadata.user_tags", "metadata.priority" ]
            }
        }
    }

    # filter inactive
    hits = [i['fields']['partial'][0] for i in query_es(query) 
            if 'inactive' not in i['fields']['partial'][0].get('metadata', {}).get('user_tags', [])]
    #logger.info("hits: {}".format(json.dumps(hits, indent=2)))
    #logger.info("aois: {}".format(json.dumps([i['id'] for i in hits])))
    return hits

def query_aois_new(starttime, endtime):
    """Query ES for active AOIs that intersect starttime and endtime."""

    es_index = "grq_*_area_of_interest"
    query = {
        "query": {
            "bool": {
                "should": [
                    {
                        "bool": {
                            "must": [
                                {
                                    "range": {
                                        "starttime": {
                                            "lte": endtime
                                        }
                                    }
                                },
                                {
                                    "range": {
                                        "endtime": {
                                            "gte": starttime
                                        }
                                    }
                                },
				{
                                    "match": {
					    "dataset_type": "area_of_interest"
                                        }
                                },
                                {
                                    "match": {
                                            "metadata.user_tags": "standard_product"
                                        }
                                }
                            ],
                 	    "must_not": {
                		"term": {
                    		    "metadata.user_tags": "inactive"
                		}
            		    }
			  
                        }
                    },
        {
        "filtered": {
            "query": {

              "bool": {
                "must": [
                  {
                    "match": {
                      "dataset_type": "area_of_interest"
                      }
                  },
                  {
                    "range": {
                      "starttime": {
                        "lte": endtime
                      }
                    }
                  },
                  {
                      "match": {
                          "metadata.user_tags": "standard_product"
                      }
                  }
                ],
       	    	"must_not": {
                    "term": {
                        "metadata.user_tags": "inactive"
                    }
                }
              }
            },
            "filter": {
              "missing": {
                "field": "endtime"
              }
            }
          }
        },
        {
        "filtered": {
            "query": {

              "bool": {
                "must": [
                  {
                    "match": {
                      "dataset_type": "area_of_interest"
                      }
                  },
                  {
                    "range": {
                      "endtime": {
                        "gte": starttime
                      }
                     }
                  },
                  {
                      "match": {
                          "metadata.user_tags": "standard_product"
                      }
                  }
                ],
           	"must_not": {
                    "term": {
                        "metadata.user_tags": "inactive"
                    }
                }
              }
            },
            "filter": {
              "missing": {
                "field": "starttime"
              }
            }
          }
        }
          
      ]
    }
  },
        "partial_fields" : {
            "partial" : {
                "include" : [ "id", "starttime", "endtime", "location", 
                              "metadata.user_tags", "metadata.priority" ]
            }
        }
    }

    # filter inactive
    hits = [i['fields']['partial'][0] for i in query_es(query) 
            if 'inactive' not in i['fields']['partial'][0].get('metadata', {}).get('user_tags', [])]
    #logger.info("hits: {}".format(json.dumps(hits, indent=2)))
    #logger.info("aois: {}".format(json.dumps([i['id'] for i in hits])))
    return hits

def get_query(acq):
    query = {
    	"query": {
    	    "filtered": {
      		"filter": {
        	    "and": [
          		{
            		    "term": {
              			"system_version.raw": "v1.1"
            		    }
          		}, 
          		{
            		    "ids": {
              			"values": [acq['identifier']]
            		    }
          		}, 
          		{
            		    "geo_shape": {
              			"location": {
                		    "shape": acq['metadata']['location']
              			}
            		    }
          		}
        	    ]
      		}, 
      		"query": {
        	    "bool": {
          		"must": [
            		    {
              			"term": {
                		    "dataset.raw": "S1-IW_SLC"
              			}
            		    }
          		]
        	    }
      		}
    	    }
  	}, 
  	"partial_fields": {
    	    "partial": {
      		"exclude": "city"
    	    }
  	}
    }

    
    return query

def get_query(acq):
    query = {
    	"query": {
    	    "filtered": {
      		"filter": {
        	    "and": [
          		{
            		    "geo_shape": {
              			"location": {
                		    "shape": acq['metadata']['location']
              			}
            		    }
          		}
        	    ]
      		}, 
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
      		}
    	    }
  	}, 
  	"partial_fields": {
    	    "partial": {
      		"exclude": "city"
    	    }
  	}
    }

    
    return query

def get_query2(acq):
    query = {
    	"query": {
    	    "filtered": {
      		"filter": {
        	    "and": [
          		{
            		    "term": {
              			"system_version.raw": "v1.1"
            		    }
          		}, 
          		{
            		    "ids": {
              			"values": [acq['identifier']]
            		    }
          		}, 
          		{
            		    "geo_shape": {
              			"location": {
                		    "shape": acq['metadata']['location']
              			}
            		    }
          		}
        	    ]
      		}, 
      		"query": {
        	    "bool": {
          		"must": [
            		    {
              			"term": {
                		    "dataset.raw": "S1-IW_SLC"
              			}
            		    }
          		]
        	    }
      		}
    	    }
  	}, 
  	"partial_fields": {
    	    "partial": {
      		"exclude": "city"
    	    }
  	}
    }

    
    return query



def get_dem_type(acq):
    dem_type = "SRTM+v3"
    if acq['city'] is not None and len(acq['city'])>0:
        if acq['city'][0]['country_name'] is not None and acq['city'][0]['country_name'].lower() == "united states":
            dem_type="Ned1"
    return dem_type

def getUpdatedTime(s, m):
    #date = dateutil.parser.parse(s, ignoretz=True)
    #new_date = s + timedelta(minutes = m)
    new_date = s + timedelta(minutes = m)
    return new_date

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
    #logger.info("intersection between :\n %s\n%s" %(js1, js2))
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

    logger.info("gt_footprint : %s:" %gt_footprint)
    geojson = {"type":"Polygon", "coordinates": [gt_footprint]}
    return geojson

def get_area_from_orbit_file(tstart, tend, orbit_file, aoi_location):
    water_percentage = 0
    land_percentage = 0
    logger.info("tstart : %s  tend : %s" %(tstart, tend))
    geojson = get_groundTrack_footprint(tstart, tend, orbit_file)
    intersection, int_env = util.get_intersection(aoi_location, geojson)
    logger.info("intersection : %s" %intersection)
    land_percentage = lightweight_water_mask.get_land_percentage(intersection)
    logger.info("get_land_percentage(geojson) : %s " %land_percentage)
    water_percentage = lightweight_water_mask.get_water_percentage(intersection)

    logger.info("covers_land : %s " %lightweight_water_mask.covers_land(geojson))
    logger.info("covers_water : %s "%lightweight_water_mask.covers_water(geojson))
    logger.info("get_land_percentage(geojson) : %s " %land_percentage)
    logger.info("get_water_percentage(geojson) : %s " %water_percentage)    
    

    return land_percentage, water_percentage

def get_area_from_acq_location(geojson):
    logger.info("geojson : %s" %geojson)
    land_percentage = lightweight_water_mask.get_land_percentage(geojson)
    water_percentage = lightweight_water_mask.get_water_percentage(geojson)

    logger.info("covers_land : %s " %lightweight_water_mask.covers_land(geojson))
    logger.info("covers_water : %s "%lightweight_water_mask.covers_water(geojson))
    logger.info("get_land_percentage(geojson) : %s " %land_percentage)
    logger.info("get_water_percentage(geojson) : %s " %water_percentage)                                    
    

    return land_percentage, water_percentage

def update_grq(acq_id, pv):
    pass

def isTrackSelected(land, water, land_area, water_area):
    selected = False
    total_acq_land = 0

    for acq_land in land:
        total_acq_land+= acq_land

    if ((total_acq_land*100)/land)> 98:
        selected = True

    return selected
        
 
def get_covered_acquisitions(aoi, acqs, orbit_file):
    
    logger.info("AOI : %s" %aoi['location'])
    grouped_matched = util.group_acqs_by_track(acqs)
    matched_ids = grouped_matched["acq_info"].keys()
           
    logger.info("grouped_matched : %s" %grouped_matched)
    logger.info("matched_ids : %s" %matched_ids)


    selected_track_acqs = {}

    for track in grouped_matched["grouped"]:
        selected = False
        starttimes = []
        endtimes = []
        polygons = []
        orbit_polygons = []
        land_area = []
        water_area = []
        
        track_acq_ids = grouped_matched["grouped"][track]
        #logger.info("%s : %s\n" %(track, grouped_matched["grouped"][track]))
        for acq_id in track_acq_ids:
            logger.info("%s : %s" %(track, acq_id))
            acq = grouped_matched["acq_info"][acq_id]
            starttimes.append(get_time(acq.starttime))
            endtimes.append(get_time(acq.endtime)) 
            polygons.append(acq.location)
            #land, water = util.get_area_from_orbit_file(get_time(acq.starttime), get_time(acq.endtime), orbit_file, aoi['location'])
            #land_area.append(land)
            #water_area.append(water)
            logger.info("acq.location : %s\n" %acq.location)    
            intersection, int_env = util.get_intersection(aoi['location'], acq.location)
            logger.info("intersection : %s" %intersection)
            #land_a, area_a = util.get_area_from_acq_location(acq.location)
        logger.info("starttimes : %s" %starttimes)
        logger.info("endtimes : %s" %endtimes)
        #get lowest starttime minus 10 minutes as starttime
        tstart = getUpdatedTime(sorted(starttimes)[0], -10)
        logger.info("tstart : %s" %tstart)
        tend = getUpdatedTime(sorted(endtimes, reverse=True)[0], 10)
        logger.info("tend : %s" %tend)
        land, water = util.get_area_from_orbit_file(tstart, tend, orbit_file, aoi['location'])
        
        ''' WE WILL NOT USE UNION GEOJSON
        union_geojson = get_union_geometry(polygons)
        logger.info("union_geojson : %s" %union_geojson)
        intersection, int_env = get_intersection(aoi['location'], union_geojson)
        logger.info("union intersection : %s" %intersection)
        #get highest entime plus 10 minutes as endtime
        tend = getUpdatedTime(sorted(endtimes, reverse=True)[0], 10)
        logger.info("endtime : %s" %endtime)
        land, water = util.get_area_from_orbit_file(tstart, tend, orbit_file, aoi['location'])
        '''


        #ADD THE SELECTION LOGIC HERE

        selected = False
        #selected = isTrackSelected(land, water, land_area, water_area)
        selected = True

        if selected:
            logger.info("SELECTED")
            selected_acqs = []
            for acq_id in track_acq_ids:
                acq = grouped_matched["acq_info"][acq_id]
                if not acq.pv:
                    acq.pv = get_processing_version(acq.identifier)
                    update_grq(acq_id, acq.pv)
                logger.info("APPENDING : %s" %acq_id)
                selected_acqs.append(acq)
            selected_track_acqs[track] = selected_acqs      

        

    #exit (0)

    return selected_track_acqs

def query_aoi_acquisitions(starttime, endtime, platform, orbit_file):
    """Query ES for active AOIs that intersect starttime and endtime and 
       find acquisitions that intersect the AOI polygon for the platform."""
    #aoi_acq = {}
    orbit_aoi_data = {}
    es_index = "grq_*_*acquisition*"
    aois = query_aois(starttime, endtime)
    logger.info("No of AOIs : %s " %len(aois))
    for aoi in aois:
        logger.info("aoi: {}".format(aoi['id']))
        query = {
            "query": {
                "filtered": {
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "term": {
                                        "dataset_type.raw": "acquisition"
                                    }
                                },
                                {
                                    "term": {
                                        "metadata.platform.raw": platform
                                    }
                                },
                                {
                                    "range": {
                                        "starttime": {
                                            "lte": endtime
                                        }
                                    }
                                },
                                {
                                    "range": {
                                        "endtime": {
                                            "gte": starttime
                                        }
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
                    "include" : [ "id", "dataset_type", "dataset", "metadata", "city", "continent", "starttime", "endtime"]
                }
            }
        }
        logger.info(query)
        acqs = [i['fields']['partial'][0] for i in query_es(query, es_index)]
        logger.info("Found {} acqs for {}: {}".format(len(acqs), aoi['id'],
                    json.dumps([i['id'] for i in acqs], indent=2)))

        #logger.info("ALL ACQ of AOI : \n%s" %acqs)

        selected_track_acqs = {}
        try:
            selected_track_acqs = get_covered_acquisitions(aoi, acqs, orbit_file)
        except Exception as  err:
            logger.info("Error from get_covered_acquisitions: %s " %str(err))
            traceback.print_exc()
        #for acq in acqs:
        aoi_data = {}
        aoi_priority = aoi.get('metadata', {}).get('priority', 0)
        # ensure highest priority is assigned if multiple AOIs resolve the acquisition
        #if acq['id'] in acq_info and acq_info[acq['id']].get('priority', 0) > aoi_priority:
            #continue
        aoi_data['aoi_id'] = aoi['id']
        aoi_data['aoi_location'] =  aoi['location']
        aoi_data['priority'] = aoi_priority
        aoi_data['selected_track_acqs'] = selected_track_acqs
        orbit_aoi_data[aoi['id']] = aoi_data
        #acq_info[aoi_data['id']] = acq
	#aoi_acq[aoi] = acq_info 
        #logger.info("Acquistions to localize: {}".format(json.dumps(acq_info, indent=2)))
    return orbit_aoi_data
    

def resolve_s1_slc(identifier, download_url, project):
    """Resolve S1 SLC using ASF datapool (ASF or NGAP). Fallback to ESA."""

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

def get_temporal_baseline(ctx):
    temporalBaseline = 24
    if 'temporalBaseline' in ctx:
        temporalBaseline = int(ctx['temporalBaseline'])
    return temporalBaseline
   

def resolve_s1_slc(identifier, download_url, project):
    """Resolve S1 SLC using ASF datapool (ASF or NGAP). Fallback to ESA."""

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

def get_processing_version(slc):
    return get_processing_version_from_asf(slc)

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




class DatasetExists(Exception):
    """Exception class for existing dataset."""
    pass

def get_temporal_baseline(ctx):
    temporalBaseline = 24
    if 'temporalBaseline' in ctx:
        temporalBaseline = int(ctx['temporalBaseline'])
    return temporalBaseline

def resolve_aoi_acqs(ctx_file):
    """Resolve best URL from acquisitions from AOIs."""

    # read in context
    with open(ctx_file) as f:
        ctx = json.load(f)

    #SFL = os.path.join(os.environ['HOME'], 'standard_product', 'aoi_acquisition_localizer_standard_product.sf.xml')
    # get acq_info


    #Find Orbit File Info
    orbit_file = None
    orbit_file_dir =os.path.basename(ctx["localize_urls"][0]["url"])
    for file in os.listdir(orbit_file_dir):
        if file.endswith(".EOF"):
            orbit_file = os.path.join(orbit_file_dir, file)

    if not orbit_file:
        raise("Orbit File NOT Found")
    else:
        logger.info("Orbit File : %s " %orbit_file)


    orbit_aoi_data = query_aoi_acquisitions(ctx['starttime'], ctx['endtime'], ctx['platform'], orbit_file)


    #logger.info(orbit_aoi_data)
    #exit(0)
    
    # build args
    #queue = ctx["recommended-queues"][0]
    queue = "system-jobs-queue"
    singlesceneOnly = True
    precise_orbit_only = True
    spyddder_extract_version= ctx['spyddder_extract_version']
    acquisition_localizer_version = ctx['acquisition_localizer_version']
    standard_product_localizer_version = ctx['standard_product_localizer_version']
    standard_product_ifg_version = 'standard-product'
    if 'standard_product_ifg_version' in ctx and ctx['standard_product_ifg_version'] is not None and ctx['standard_product_ifg_version'] !="":
        standard_product_ifg_version = ctx['standard_product_ifg_version']

    #standard_product_version= ctx['standard_product_version']
    project = ctx['project']
    logger.info("PROJECT : %s" %project)
    priority = ctx["job_priority"]
    job_type, job_version = ctx['job_specification']['id'].split(':') 

    job_data = {}
 
    job_data["project"] = project
    job_data["spyddder_extract_version"] = spyddder_extract_version
    job_data["standard_product_ifg_version"] = standard_product_ifg_version
    job_data["acquisition_localizer_version"] = acquisition_localizer_version
    job_data["standard_product_localizer_version"] = standard_product_localizer_version
    job_data["job_type"] = job_type
    job_data["job_version"] = job_version
    job_data["job_priority"] = ctx['job_priority']
    

    orbit_acq_selections = {}
    orbit_acq_selections["job_data"] = job_data
    orbit_acq_selections["orbit_aoi_data"] = orbit_aoi_data

    return orbit_acq_selections


    '''
    acquisitions = []
    acquisition_array =[]

    for id in sorted(acq_info):
        acq = acq_info[id]
        aoi = acq['aoi']
        pv = None
        if "processing_version" in  acq['metadata']:
            pv = acq['metadata']['processing_version']
            pv2 = get_processing_version(acq['metadata']['identifier'])
            logger.info("pv : %s, pv2 : %s" %(pv, pv2))
        else:
            pv = get_processing_version(acq['metadata']['identifier'])
        logging.info("\n\nPrinting AOI : %s, Acq : %s" %(aoi, id))
            #print(acq)

    	#job_type = "job-standard_product_localizer:{}".format(standard_product_localizer_version)
       
	    #return id, project, spyddder_extract_version, aoi, priority, queue


        acq_data = {
            "acq_id" : id,
            "project" : project,
            #"identifier" : acq['metadata']['identifier'],
	    "spyddder_extract_version" : spyddder_extract_version,
	    "standard_product_ifg_version" : standard_product_ifg_version,
	    "acquisition_localizer_version" : acquisition_localizer_version,
	    "standard_product_localizer_version" : standard_product_localizer_version,
  	    #"job_type" : job_type, 
	    #"job_version" : job_version,
	    "job_priority" : ctx['job_priority']
		
        } 

        #logger.info("\n\nacq data for :%s :\n%s" %(id, acq_data))
        acquisition_array.append(acq_data)

    acquisitions.append(acquisition_array)

    logging.info("acquisition_array length : %s" %acquisition_array)
    #return acquisitions
    return acquisition_array

   
        logging.info("acq identifier : %s " %identifier)
	logging.info("acq city : %s " %acq['city'])
        logging.info("dem_type : %s" %dem_type)
	logging.info("query : %s" %query)
	logging.info("bbox : %s" %bbox)
        logging.info("ipf : %s" %ipf)
	
        acquisition_array.append([project, True, bbox, dataset, identifier, download_url, dataset_type, ipf, archive_filename, query, aoi, dem_type, spyddder_extract_version, standard_product_version, queue, job_priority, preReferencePairDirection, postReferencePairDirection, temporalBaseline, singlesceneOnly, precise_orbit_only])
        
        return project, True, bbox, dataset, identifier, download_url, dataset_type, ipf, archive_filename, query, aoi, dem_type, spyddder_extract_version, standard_product_version, queue, job_priority, preReferencePairDirection, postReferencePairDirection, temporalBaseline, singlesceneOnly, precise_orbit_only
        exit(0)
        #job = resolve_hysds_job(job_type, queue, priority=acq['priority'], params=params, job_name="%s-%s-%s" % (job_type, aoi, prod_name))

        #job_id = submit_hysds_job(job)

    '''

def main():

    # read in _context.json
    context_file = os.path.abspath("_context.json")
    if not os.path.exists(context_file):
        raise(RuntimeError("Context file doesn't exist."))
    
    resolve_aoi_acqs(context_file)

if __name__ == "__main__":
    sys.exit(main())

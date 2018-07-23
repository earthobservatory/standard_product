#!/usr/bin/env python 
import os, sys, time, json, requests, logging

from hysds_commons.job_utils import resolve_hysds_job
from hysds.celery import app


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


def query_es(query, es_index):
    """Query ES."""

    es_url = app.conf.GRQ_ES_URL
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
                                }
                            ]
                        }
                    },
                    {
                        "filtered": {
                            "query": {
                                "range": {
                                    "starttime": {
                                        "lte": endtime
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
                                "range": {
                                    "endtime": {
                                        "gte": starttime
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
    hits = [i['fields']['partial'][0] for i in query_es(query, es_index) 
            if 'inactive' not in i['fields']['partial'][0].get('metadata', {}).get('user_tags', [])]
    #logger.info("hits: {}".format(json.dumps(hits, indent=2)))
    logger.info("aois: {}".format(json.dumps([i['id'] for i in hits])))
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

def get_dem_type(acq):
    dem_type = "SRTM+v3"
    if acq['city'] is not None and len(acq['city'])>0:
	if acq['city'][0]['country_name'] is not None and acq['city'][0]['country_name'].lower() == "united states":
	    dem_type="Ned1"
    return dem_type

def query_aoi_acquisitions(starttime, endtime, platform):
    """Query ES for active AOIs that intersect starttime and endtime and 
       find acquisitions that intersect the AOI polygon for the platform."""

    acq_info = {}
    es_index = "grq_*_*acquisition*"
    for aoi in query_aois(starttime, endtime):
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
                    "include" : [ "id", "dataset_type", "dataset", "metadata", "city", "continent" ]
                }
            }
        }
        #print(query)
        acqs = [i['fields']['partial'][0] for i in query_es(query, es_index)]
        logger.info("Found {} acqs for {}: {}".format(len(acqs), aoi['id'],
                    json.dumps([i['id'] for i in acqs], indent=2)))
        for acq in acqs:
            aoi_priority = aoi.get('metadata', {}).get('priority', 0)
            # ensure highest priority is assigned if multiple AOIs resolve the acquisition
            if acq['id'] in acq_info and acq_info[acq['id']].get('priority', 0) > aoi_priority:
                continue
            acq['aoi'] = aoi['id']
            acq['priority'] = aoi_priority
            acq_info[acq['id']] = acq
    logger.info("Acquistions to localize: {}".format(json.dumps(acq_info, indent=2)))
    return acq_info
    

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

def resolve_aoi_acqs(ctx_file):
    """Resolve best URL from acquisitions from AOIs."""

    # read in context
    with open(ctx_file) as f:
        ctx = json.load(f)

    SFL = os.path.join(os.environ['HOME'], 'standard_product', 'aoi_acquisition_localizer_standard_product.sf.xml')
    # get acq_info
    acq_info = query_aoi_acquisitions(ctx['starttime'], ctx['endtime'], ctx['platform'])

    # build args
    spyddder_extract_versions = []
    queues = []
    urls = []
    archive_filenames = []
    identifiers = []
    prod_dates = []
    priorities = []
    aois = []
    temporalBaseline = get_temporal_baseline(ctx)
    queue = ctx['queue']
    singlesceneOnly = True
    precise_orbit_only = True
    for id in sorted(acq_info):
        acq = acq_info[id]
        logging.info("\n\nPrinting Acq : %s" %id)
        #print(acq)
        acq['spyddder_extract_version'] = ctx['spyddder_extract_version']
        acq['standard_product_version'] = ctx['standard_product_version']
        acq['project'] = ctx['project']
        acq['identifier'] = acq['metadata']['identifier']
        acq['download_url'] = acq['metadata']['download_url']
        acq['archive_filename'] = acq['metadata']['archive_filename']
        acq['aoi'] = acq['aoi']
        acq['job_priority'] = acq['priority']

        job_type = "sciflo_stage_iw_slc:{}".format(ctx['stage_iw_slc_version'])
	preReferencePairDirection = "backward"
	postReferencePairDirection = "backward"
        params = {
        "dataset" : acq['dataset'],
        "project" : acq['project'],
        "identifier" : acq['identifier'],
        "download_url" : acq['download_url'],
        "dataset_type" : acq['dataset_type'],
	"archive_filename" : acq['archive_filename'],
	"spyddder_extract_version" : acq['spyddder_extract_version'],
	"standard_product_version" : acq['standard_product_version'],
	"aoi" : acq['aoi'],
	"job_priority" : acq['job_priority']
        }
        logging.info("acq identifier : %s " %acq['identifier'])
	logging.info("acq location : %s " %acq['metadata']['location'])
        logging.info("acq continent : %s " %acq['continent'])
	logging.info("acq city : %s " %acq['city'])
	dem_type = get_dem_type(acq)
	query = get_query(acq)
        logging.info("dem_type : %s" %dem_type)
	logging.info("query : %s" %query)
	
        return "standard_product", True, query, acq['aoi'], dem_type, acq['spyddder_extract_version'], acq['standard_product_version'], queue, acq['priority'], preReferencePairDirection, postReferencePairDirection, temporalBaseline, singlesceneOnly, precise_orbit_only

        #job = resolve_hysds_job(job_type, queue, priority=acq['priority'], params=params, job_name="%s-%s-%s" % (job_type, aoi, prod_name))

        #job_id = submit_hysds_job(job)


def main():
    """Run S1 create interferogram sciflo."""

    # read in _context.json
    context_file = os.path.abspath("_context.json")
    if not os.path.exists(context_file):
        raise(RuntimeError("Context file doesn't exist."))
    
    resolve_aoi_acqs(context_file)

if __name__ == "__main__":
    sys.exit(main())

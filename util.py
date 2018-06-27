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


def dataset_exists(id, index_suffix):
    """Query for existence of dataset by ID."""

    # es_url and es_index
    es_url = app.conf.GRQ_ES_URL
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
    es_url = app.conf.GRQ_ES_URL
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


def resolve_source(ctx):
    """Resolve best URL from acquisition."""

    # get settings
    settings_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'settings.json')
    with open(settings_file) as f:
        settings = json.load(f)

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
        url, queue = resolve_s1_slc(ctx['identifier'], ctx['download_url'], ctx['project'])
    else:
        raise NotImplementedError("Unknown acquisition dataset: {}".format(ctx['dataset']))

    return ( ctx['spyddder_extract_version'], queue, url, ctx['archive_filename'], 
             ctx['identifier'], time.strftime('%Y-%m-%d' ), ctx.get('job_priority', 0),
             ctx.get('aoi', 'no_aoi') )


def resolve_source_from_ctx_file(ctx_file):
    """Resolve best URL from acquisition."""

    with open(ctx_file) as f:
        return resolve_source(json.load(f))


def extract_job(spyddder_extract_version, queue, localize_url, file, prod_name,
                prod_date, priority, aoi, wuid=None, job_num=None):
    """Map function for spyddder-man extract job."""

    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")

    # set job type and disk space reqs
    job_type = "job-spyddder-extract:{}".format(spyddder_extract_version)

    # resolve hysds job
    params = {
        "localize_url": localize_url,
        "file": file,
        "prod_name": prod_name,
        "prod_date": prod_date,
        "aoi": aoi,
    }
    job = resolve_hysds_job(job_type, queue, priority=priority, params=params, 
                            job_name="%s-%s-%s" % (job_type, aoi, prod_name))

    # save to archive_filename if it doesn't match url basename
    if os.path.basename(localize_url) != file:
        job['payload']['localize_urls'][0]['local_path'] = file

    # add workflow info
    job['payload']['_sciflo_wuid'] = wuid
    job['payload']['_sciflo_job_num'] = job_num
    print("job: {}".format(json.dumps(job, indent=2)))

    return job

def standard_product_job(standard_product_version, queue, prod_name,
                prod_date, priority, aoi, wuid=None, job_num=None):
    # set job type and disk space reqs
    job_type = "job-standard-product:{}".format(standard_product_version)
    
    # resolve hysds job
    params = {
        "singlesceneOnly": True,
        "preReferencePairDirection": "",
        "postReferencePairDirection": "",
        "minMatch": 2,
        "covth": 0.95,
        "preciseOrbitOnly": True,
        "range_looks": 7,
	"azimuth_looks": 19,
	"filter_strength": 0.5
    }

    job = resolve_hysds_job(job_type, queue, priority=priority, params=params, job_name="%s-%s-%s" % (job_type, aoi, prod_name))

    return job
        
    

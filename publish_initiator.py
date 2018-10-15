import os, sys, re, requests, json, logging, traceback, argparse, copy, bisect
import hashlib
from itertools import product, chain
from datetime import datetime, timedelta
#from hysds.celery import app
import random
#from random import randint
from osgeo import ogr
import util

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

IFG_ID_TMPL = "S1-IFG_R{}_M{:d}S{:d}_TN{:03d}_{:%Y%m%dT%H%M%S}-{:%Y%m%dT%H%M%S}_s123-{}-{}-standard_product"
RSP_ID_TMPL = "S1-SLCP_R{}_M{:d}S{:d}_TN{:03d}_{:%Y%m%dT%H%M%S}-{:%Y%m%dT%H%M%S}_s{}-{}-{}"

BASE_PATH = os.path.dirname(__file__)
MOZART_ES_ENDPOINT = "MOZART"
GRQ_ES_ENDPOINT = "GRQ"
GRQ_ES_URL = "http://100.64.134.208:9200/"


def query_grq( doc_id):
    """
    This function queries ES
    :param endpoint: the value specifies which ES endpoint to send query
     can be MOZART or GRQ
    :param doc_id: id of product or job
    :return: result from elasticsearch
    """
    es_url, es_index = None, None

    '''
    if endpoint == GRQ_ES_ENDPOINT:
        es_url = app.conf["GRQ_ES_URL"]
        es_index = "grq"
    if endpoint == MOZART_ES_ENDPOINT:
        es_url = app.conf['JOBS_ES_URL']
        es_index = "job_status-current"
    '''
    # get normalized rest url
    es_url = GRQ_ES_URL
    #rest_url = es_url[:-1] if es_url.endswith('/') else es_url
    es_index = "grq"

    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"_id": doc_id}} # add job status:
                ]
            }
        }
    }
    #print(query)

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



def get_version():
    """Get dataset version."""

    DS_VERS_CFG = os.path.normpath(
                      os.path.join(
                          os.path.dirname(os.path.abspath(__file__)),
                          '..', '..', 'conf', 'dataset_versions.json'))
    with open(DS_VERS_CFG) as f:
        ds_vers = json.load(f)
    return ds_vers['Standard-Product']


def get_dem_type(info):
    """Get dem type."""

    dem_type = "SRTM+v3"

    dems = {}
    for id in info:
        dem_type = "SRTM+v3"
        h = info[id]
        fields = h["_source"]
        try:
            if 'city' in fields:
                if fields['city'][0]['country_name'] is not None and fields['city'][0]['country_name'].lower() == "united states":
                    dem_type="Ned1"
                dems.setdefault(dem_type, []).append(id)
        except:
            dem_type = "SRTM+v3"

    if len(dems) != 1:
        logger.info("There are more than one type of dem, so selecting SRTM+v3")
        dem_type = "SRTM+v3"
    return dem_type


def get_metadata(id, rest_url, url):
    """Get SLC metadata."""

    # query hits
    query = {
        "query": {
            "term": {
                "_id": id
            }
        }
    }
    logger.info("query: {}".format(json.dumps(query, indent=2)))
    r = requests.post(url, data=json.dumps(query))
    r.raise_for_status()
    scan_result = r.json()
    count = scan_result['hits']['total']
    scroll_id = scan_result['_scroll_id']
    hits = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' % rest_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        hits.extend(res['hits']['hits'])
    if len(hits) == 0:
        raise RuntimeError("Failed to find {}.".format(id))
    return hits[0]


def get_bool_param(ctx, param):
    """Return bool param from context."""

    if param in ctx and isinstance(ctx[param], bool): return ctx[param]
    return True if ctx.get(param, 'true').strip().lower() == 'true' else False

def get_track(info):
    """Get track number."""

    tracks = {}
    for id in info:
        logger.info(id)
        h = info[id]
        fields = h["_source"]
        track = fields['metadata']['trackNumber']
        logger.info(track)
        tracks.setdefault(track, []).append(id)
    if len(tracks) != 1:
        print(tracks)
        
        raise RuntimeError("Failed to find SLCs for only 1 track : %s" %tracks)
    return track


def create_dataset_json2(id, version, met_file, ds_file):
    """Write dataset json."""


    # get metadata
    with open(met_file) as f:
        md = json.load(f)

    print("create_dataset_json : met['bbox']: %s" %md['bbox'])
    coordinates = [
                    [
                      [ md['bbox'][0][1], md['bbox'][0][0] ],
                      [ md['bbox'][3][1], md['bbox'][3][0] ],
                      [ md['bbox'][2][1], md['bbox'][2][0] ],
                      [ md['bbox'][1][1], md['bbox'][1][0] ],
                      [ md['bbox'][0][1], md['bbox'][0][0] ]
                    ] 
                  ]
    cord_area = util.get_area(coordinates[0])
    if not cord_area>0:
        logger.info("creating dataset json. coordinates are not clockwise, reversing it")
        coordinates = [coordinates[0][::-1]] 
        logger.info(coordinates)
        cord_area = util.get_area(coordinates[0])
        if not cord_area>0:
            logger.info("creating dataset json. coordinates are STILL NOT  clockwise")
    else:
        logger.info("creating dataset json. coordinates are already clockwise")
            
    # build dataset
    ds = {
        'creation_timestamp': "%sZ" % datetime.utcnow().isoformat(),
        'version': version,
        'label': id,
        'location': {
            'type': 'Polygon',
            'coordinates': coordinates
        }
    }

    # set earliest sensing start to starttime and latest sensing stop to endtime
    if isinstance(md['sensingStart'], str):
        ds['starttime'] = md['sensingStart']
    else:
        md['sensingStart'].sort()
        ds['starttime'] = md['sensingStart'][0]

    if isinstance(md['sensingStop'], str):
        ds['endtime'] = md['sensingStop']
    else:
        md['sensingStop'].sort()
        ds['endtime'] = md['sensingStop'][-1]

    # write out dataset json
    with open(ds_file, 'w') as f:
        json.dump(ds, f, indent=2)


def create_dataset_json(id, version, met_file, ds_file):
    """Write dataset json."""


    # get metadata
    with open(met_file) as f:
        md = json.load(f)

    ds = {
        'creation_timestamp': "%sZ" % datetime.utcnow().isoformat(),
        'version': version,
        'label': id
    }

    coordinates = None

    try:

        coordinates = md['union_geojson']['coordinates']
        cord_area = util.get_area(coordinates[0])
        if not cord_area>0:
            logger.info("creating dataset json. coordinates are not clockwise, reversing it")
            coordinates = [coordinates[0][::-1]]
            logger.info(coordinates)
            cord_area = util.get_area(coordinates[0])
            if not cord_area>0:
                logger.info("creating dataset json. coordinates are STILL NOT  clockwise")
        else:
            logger.info("creating dataset json. coordinates are already clockwise")

        ds['location'] =  {'type': 'Polygon', 'coordinates': coordinates}

    except Exception as err:
        logger.warn(str(err))
        logger.warn("Traceback: {}".format(traceback.format_exc()))


    ds['starttime'] = md['starttime']
    ds['endtime'] = md['endtime']

    # write out dataset json
    with open(ds_file, 'w') as f:
        json.dump(ds, f, indent=2)


def publish_initiator(candidate_pair_list, job_data):
    for candidate_pair in candidate_pair_list:
        publish_initiator_pair(candidate_pair, job_data)
    
'''
def publish_initiator( master_acquisitions, slave_acquisitions, project, spyddder_extract_version, acquisition_localizer_version, standard_product_localizer_version, standard_product_ifg_version, job_priority, wuid=None, job_num=None):
    for i in range(len(master_acquisitions)):
        publish_initiator_pair( master_acquisitions[i], slave_acquisitions[i], project[i], spyddder_extract_version[i], acquisition_localizer_version[i], standard_product_localizer_version[i], standard_product_ifg_version[i], job_priority[i])

def publish_initiator_pair( master_acquisitions, slave_acquisitions, project, spyddder_extract_version, acquisition_localizer_version, standard_product_localizer_version, standard_product_ifg_version, job_priority, wuid=None, job_num=None):
'''
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
    spyddder_extract_version = job_data["spyddder_extract_version"] 
    standard_product_ifg_version = job_data["standard_product_ifg_version"] 
    acquisition_localizer_version = job_data["acquisition_localizer_version"]
    standard_product_localizer_version = job_data["standard_product_localizer_version"] 
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
    master_md = { i:get_metadata(i, rest_url, url) for i in master_acquisitions }
    #logger.info("master_md: {}".format(json.dumps(master_md, indent=2)))
    slave_md = { i:get_metadata(i, rest_url, url) for i in slave_acquisitions }
    #logger.info("slave_md: {}".format(json.dumps(slave_md, indent=2)))

    # get tracks
    track = get_track(master_md)
    logger.info("master_track: {}".format(track))
    slave_track = get_track(slave_md)
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
 

    dem_type = get_dem_type(master_md)

    # get dem_type
    dem_type = get_dem_type(master_md)
    logger.info("master_dem_type: {}".format(dem_type))
    slave_dem_type = get_dem_type(slave_md)
    logger.info("slave_dem_type: {}".format(slave_dem_type))
    if dem_type != slave_dem_type:
        dem_type = "SRTM+v3"


 
    job_queue = "%s-job_worker-large" % project
    logger.info("submit_localize_job : Queue : %s" %job_queue)

    localizer_job_type = "job-standard_product_localizer:%s" % standard_product_localizer_version

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


    id_hash = hashlib.md5(json.dumps([
            job_priority,
            master_ids_str,
            slave_ids_str
    ]).encode("utf8")).hexdigest()


    id = "standard-product-ifg-acq-%s" %id_hash[0:4]
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
    md['spyddder_extract_version'] = spyddder_extract_version
    md['acquisition_localizer_version'] = acquisition_localizer_version
    md['standard_product_ifg_version'] = standard_product_ifg_version
    md['job_priority'] = job_priority
    md['_disk_usage'] = disk_usage
    md['soft_time_limit'] =  86400
    md['time_limit'] = 86700
    md['dem_type'] = dem_type
    md['track'] = track
    md['starttime'] = "%sZ" %starttime
    md['endtime'] = "%sZ" %endtime
    md['union_geojson'] = union_geojson
    
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



def submit_localize_job( master_acquisitions, slave_acquisitions, project, spyddder_extract_version, acquisition_localizer_version, standard_product_localizer_version, standard_product_ifg_version, job_priority, wuid=None, job_num=None):
    """Map function for create interferogram job json creation."""

    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")



    # set job type and disk space reqs
    disk_usage = "300GB"

    # set job queue based on project
    job_queue = "%s-job_worker-large" % project
    logger.info("submit_localize_job : Queue : %s" %job_queue)

    localizer_job_type = "job-standard_product_localizer:%s" % standard_product_localizer_version
    master_ids_str=""
    slave_ids_str=""

    logger.info("master acq type : %s of length %s"  %(type(master_acquisitions), len(master_acquisitions)))
    logger.info("slave acq type : %s of length %s" %(type(slave_acquisitions), len(master_acquisitions)))


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

    logger.info("Master Acquisitions_str : %s" %master_ids_str)
    logger.info("Slave Acquisitions_str : %s" %slave_ids_str)

    job_hash = hashlib.md5(json.dumps([
        job_priority,
        master_ids_str,
        slave_ids_str
    ]).encode("utf8")).hexdigest()
    
    return {
        "job_name": "%s-%s" % (localizer_job_type, job_hash[0:4]),
        "job_type": localizer_job_type,
        "job_queue": job_queue,
        "container_mappings": {
            "/home/ops/.netrc": "/home/ops/.netrc",
            "/home/ops/.aws": "/home/ops/.aws"
            #"/home/ops/ariamh/conf/settings.conf": "/home/ops/ariamh/conf/settings.conf"
        },    
        "soft_time_limit": 86400,
        "time_limit": 86700,
        "payload": {
            # sciflo tracking info
            "_sciflo_wuid": wuid,
            "_sciflo_job_num": job_num,

            # job params
            "project": project,
            "master_acquisitions": master_ids_str,
	    "slave_acquisitions": slave_ids_str,
	    "spyddder_extract_version" : spyddder_extract_version,
	    "acquisition_localizer_version" : acquisition_localizer_version,
	    "standard_product_ifg_version" : standard_product_ifg_version,
	    "job_priority" : job_priority,

            # v2 cmd
            "_command": "/home/ops/verdi/ops/standard_product/sciflo_stage_iw_slc.sh",

            # disk usage
            "_disk_usage": disk_usage

        }
    }


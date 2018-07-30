#!/usr/bin/env python 
import os, sys, time, json, requests, logging

from hysds_commons.job_utils import resolve_hysds_job
from hysds.celery import app
import util


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


def check_acq_status(acq_id, index_suffix):

    result = util.get_dataset(acq_id, index_suffix)
    total = result['hits']['total']

    if total > 0:
	return True

    return False


def resolve_aquisition_status(master_acqs, slave_acqs):

    deduped = []
    completed = []
    failed = []
    waiting = []
    master_acqs_status = {}
    slave_acqs_status = {}
    index_suffix = "S1-IW_SLC"

    for acq in master_acqs:
 	master_acqs_status[acq] = check_acq_status(acq_id, index_suffix)

    for acq in slave_acqs:
        slave_acqs_status[acq] = check_acq_status(acq_id, index_suffix)



def submit_sling_job(ds_exists, spyddder_extract_version, queue, localize_url, file, prod_name,
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
    

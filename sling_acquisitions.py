#!/usr/bin/env python 
import os, sys, time, json, requests, logging

from hysds_commons.job_utils import resolve_hysds_job
from hysds.celery import app
import util


# set logger
log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logger.setLevel(logging.INFO)
#logger.addFilter(LogFilter())

BASE_PATH = os.path.dirname(__file__)


class SLC:
    def __init__(self, slc_id, download_url, ds_status, job_id=None, job_status = None):
	self.slc_id=slc_id
	self.download_url = download_url
	self.ds_status = ds_status
        self.job_id = job_id
	self.job_status = job_status



def get_job_status(job_id):
    """
    This function gets the staged products and context of previous PGE job
    :param job_id: this is the id of the job on mozart
    :return: tuple(products_staged, prev_context, message)
    the message refects the
    """
    endpoint = MOZART_ES_ENDPOINT
    return_job_id = None
    return_job_status = None

    #check if Jobs ES has updated job status
    #if check_ES_status(job_id):
    response = query_es(endpoint, job_id)

    result = response["hits"]["hits"][0]
    message = None  #using this to store information regarding deduped jobs, used later to as error message unless it's value is "success"

    #print ("Job INFO retrieved from ES: %s"%json.dumps(result))
    #print ("Type of status from ES: %s"%type(result["_source"]["status"]))
    status = str(result["_source"]["status"])
    if  status == "job-deduped":
        logger.info("Job was deduped")
        print("Job was deduped")
        #query ES for the original job's status
        orig_job_id = result["_source"]["dedup_job"]
        return_job_id = orig_job_id
        orig_job_info = query_es(endpoint, orig_job_id)
        """check if original job failed -> this would happen when at the moment of deduplication, the original job
         was in 'running state', but soon afterwards failed. So, by the time the status is checked in this function,
         it may be shown as failed."""
        #print ("Original JOB info: \n%s"%json.dumps(orig_job_info))
        orig_job_info = orig_job_info["hits"]["hits"][0]
        orig_job_status = str(orig_job_info["_source"]["status"])
	return_job_status = orig_job_status

        if  orig_job_status == "job-failed":
            message = "Job was deduped against a failed job with id: %s, please retry job."%orig_job_id
            logger.info(message) 
        elif orig_job_status == "job-started" or orig_job_status == "job-queued":
            print ("Job was deduped against a queued/started job with id: %s. Please look at already running job with same params."%orig_job_id)
            message = "Job was deduped against a queued/started job with id: %s. Please look at already running job with same params."%orig_job_id
        elif orig_job_status == "job-completed":
            # return products staged and context of original job
            message = "success"
    else:
	return_job_id = job_id
    	return_job_status = result["_source"]["status"]

    return return_job_status, return_job_id

def check_slc_status(acq_id, index_suffix):

    result = util.get_dataset(acq_id, index_suffix)
    total = result['hits']['total']

    if total > 0:
	return True

    return False

def resolve_source():
    """Resolve best URL from acquisition."""


    # get settings

    context_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), '_context_sling.json')
    with open(context_file) as f:
        ctx = json.load(f)


    settings_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'settings.json')
    with open(settings_file) as f:
        settings = json.load(f)


    sleep_seconds = 30
    

    # build args
    spyddder_extract_versions = ctx["spyddder_extract_versions"]
    standard_product_versions = ["standard_product_versions"]
    master_acqs = ["master_acquisations"]
    slave_acqs = ["slave_acquisations"]
    queues = []  # where should we get the queue value
    identifiers = []
    prod_dates = []
    priorities = []


    slc_info = {}
    
    index_suffix = "S1-IW_SLC"



    # Find out status of all Master SLCs, create a SLC object with that and update slc_info dictionary
    for acq in master_acqs:
        logger.info(acq)
	acq_data = util.get_acquisition_data(acq)[0]['fields']['partial'][0]
	slc_id = acq_data['metadata']['identifier']
        download_url = acq_data['metadata']['download_url']
 	status = check_slc_status(slc_id, index_suffix)
        if status:
	    # status=1
            logger.info("%s exists" %slc_id)
	    slc_info[slc_id]=SLC(slc_id, download_url, 1)
	else:
	    # status=1
            logger.info("%s exists" %slc_id)
	    logger.info(download_url)
	    slc_info[slc_id]=SLC(slc_id, download_url, 0)

    # Find out status of all Slave SLCs, create a SLC object with that and update slc_info dictionary
    for acq in slave_acqs:
        logger.info(acq)
        acq_data = util.get_acquisition_data(acq)[0]['fields']['partial'][0]
        slc_id = acq_data['metadata']['identifier']
        download_url = acq_data['metadata']['download_url']
        status = check_slc_status(slc_id, index_suffix)
        if status:
	    # status=1
            logger.info("%s exists" %slc_id)
	    slc_info[slc_id]=SLC(slc_id, download_url, 1)
        else:
	    #status = 0
	    logger.info("%s does NOT exist"%slc_id)
	    slc_info[slc_id]=SLC(slc_id, download_url, 0)
            logger.info(download_url)


    # slc_info has now all the SLC's status. Now submit the Sling job for the one's whose status = 0 and update the slc_info with job id
    for slc_id in slc_info.keys():
	if not slc_info[slc_id].ds_status:
	    download_url = slc_info[slc_id].download_url
	    print ("Submitting sling job for %s" %download_url)
	    job_id = submit_sling_job(spyddder_extract_version, queue, localize_url, prod_name, prod_date, priority, aoi)
	    slc_info[slc_id].job_id = job_id
	    job_status, new_job_id  = get_job_status(job_id)
	    slc_info[slc_id].job_id = new_job_id
	    slc_info[slc_id].job_id = job_status


    # Now loop in until all the jobs are completed 
    all_done = False

    while not all_done:

        for slc_id in slc_info.keys():
            if not slc_info[slc_id].ds_status: 
		job_status, job_id  = get_job_status(slc_info[slc_id].job_id)  
  		if job_status == "job-completed":
		    logger.info("Success! sling job for slc : %  with job id : %s COMPLETED!!")
		    slc_info[slc_id].job_id = job_id
		    slc_info[slc_id].job_status = job_status
		elif job_status == "job-failed":
		    download_url = slc_info[slc_id].download_url
           	    print ("Submitting sling job for %s" %download_url)
            	    job_id = submit_sling_job(spyddder_extract_version, queue, localize_url, prod_name, prod_date, priority, aoi)
            	    slc_info[slc_id].job_id = job_id
            	    job_status, new_job_id  = get_job_status(job_id)
            	    slc_info[slc_id].job_id = new_job_id
            	    slc_info[slc_id].job_id = job_status
		else:
		    slc_info[slc_id].job_id = job_id
                    slc_info[slc_id].job_status = job_status
		    logger.info("Sling Job for SLC : %s status: "%slc_info[slc_id])
		    logger.info("Job id : %s. Job Status : %s" %(slc_info[slc_id].job_id, slc_info[slc_id].job_status))

	all_done = check_all_job_completed(slc_info)
	if not all_done:
	    time.sleep(sleep_seconds)



    #At this point, all sling jobs have been completed. Now lets recheck the slc status

    all_exists = False

    while not all_exists:
        all_exists = True
	for slc_id in slc_info.keys():
            if not slc_info[slc_id].ds_status:
 		slc_info[slc_id].ds_status = check_slc_status(slc_id, index_suffix)
		if not slc_info[slc_id].ds_status:
		    all_exists = False
		    break
	if not all_exists:
            time.sleep(5)


    # At this point, we have all the slc downloaded and we are ready to submit a create standard product job
    

        
	


def check_all_job_completed(slc_info):
    all_done = True
    for slc_id in slc_info.keys():
        if not slc_info[slc_id].status:  
	    job_status = slc_info[slc_id].job_status
	    if not job_status == "job-completed":	
		all_done = False
		break
    return all_done


#def submit_sling_job(spyddder_extract_version, queue, localize_url, file, prod_name, prod_date, priority, aoi, wuid=None, job_num=None):
def submit_sling_job(spyddder_extract_version, queue, localize_url, prod_name, prod_date, priority, aoi, wuid=None, job_num=None):
    """Map function for spyddder-man extract job."""

    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")

    # set job type and disk space reqs
    job_type = "job-spyddder-extract:{}".format(spyddder_extract_version)

    # resolve hysds job
    params = {
        "localize_url": localize_url,
        #"file": file,

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
    

def main():
    master_acqs = ["acquisition-S1A_IW_SLC__1SDV_20180702T135953_20180702T140020_022616_027345_3578"]
    slave_acqs = ["acquisition-S1B_IW_SLC__1SDV_20180720T015751_20180720T015819_011888_015E1C_3C64"]
    resolve_aquisition_status(master_acqs, slave_acqs)

if __name__ == "__main__":
    main()


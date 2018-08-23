#!/usr/bin/env python 
import os, sys, time, json, requests, logging
import hashlib
from datetime import datetime
from hysds_commons.job_utils import resolve_hysds_job
from hysds.celery import app
import util
import uuid  # only need this import to simulate returned mozart job id
from hysds.celery import app
from hysds_commons.job_utils import submit_mozart_job


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
MOZART_URL = app.conf['MOZART_URL']
MOZART_ES_ENDPOINT = "MOZART"
GRQ_ES_ENDPOINT = "GRQ"
sleep_seconds = 120
slc_check_max_sec = 300
sling_completion_max_sec = 10800


class ACQ:
    def __init__(self, acq_id, acq_type, acq_data, localized=False, job_id=None, job_status = None):
	self.acq_id=acq_id
	self.acq_type = acq_type
	self.acq_data = acq_data
	self.localized = localized
        self.job_id = job_id
	self.job_status = job_status

def get_acq_object(acq_id, acq_type, acq_data, localized=False, job_id=None, job_status = None):
    return {
        "acq_id": acq_id,
        "acq_type":  acq_type,
        "acq_data" :acq_data,
        "localized" : localized,
        "job_id": job_id,
        "job_status": job_status


    }


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
    if util.check_ES_status(job_id):
        response = util.query_es(endpoint, job_id)

    result = response["hits"]["hits"][0]
    message = None  #using this to store information regarding deduped jobs, used later to as error message unless it's value is "success"

    #print ("Job INFO retrieved from ES: %s"%json.dumps(result))
    #print ("Type of status from ES: %s"%type(result["_source"]["status"]))
    status = str(result["_source"]["status"])
    if status == "job-deduped":
        #query ES for the original job's status
        orig_job_id = result["_source"]["dedup_job"]
        return_job_id = orig_job_id
        orig_job_info = util.query_es(endpoint, orig_job_id)
        """check if original job failed -> this would happen when at the moment of deduplication, the original job
         was in 'running state', but soon afterwards failed. So, by the time the status is checked in this function,
         it may be shown as failed."""
        #print ("Original JOB info: \n%s"%json.dumps(orig_job_info))
        orig_job_info = orig_job_info["hits"]["hits"][0]
        orig_job_status = str(orig_job_info["_source"]["status"])
	logger.info("Check Job Status : Job %s was Deduped. The new/origianl job id is %s whose status is : %s" %(job_id, return_job_id, return_job_status)) 
	return_job_status = orig_job_status

        if  orig_job_status == "job-failed":
            message = "Job was deduped against a failed job with id: %s, please retry job."%orig_job_id
            logger.info(message) 
        elif orig_job_status == "job-started" or orig_job_status == "job-queued":
            logger.info ("Job was deduped against a queued/started job with id: %s. Please look at already running job with same params."%orig_job_id)
            message = "Job was deduped against a queued/started job with id: %s. Please look at already running job with same params."%orig_job_id
        elif orig_job_status == "job-completed":
            # return products staged and context of original job
            message = "success"
    else:
	return_job_id = job_id
    	return_job_status = result["_source"]["status"]

    return return_job_status, return_job_id

def check_slc_status(slc_id, index_suffix):

    result = util.get_dataset(slc_id, index_suffix)
    total = result['hits']['total']

    if total > 0:
	return True

    return False

def check_slc_status(slc_id):

    result = util.get_dataset(slc_id)
    total = result['hits']['total']

    if total > 0:
        return True

    return False


def resolve_source(ctx_file):
    """Resolve best URL from acquisition."""


    # get settings
    # read in context
    with open(ctx_file) as f:
        ctx = json.load(f)
    
    '''
    settings_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'settings.json')
    with open(settings_file) as f:
        settings = json.load(f)
    '''

    sleep_seconds = 30
    

    # build args
    project = ctx["project"]
    master_acqs = [i.strip() for i in ctx['master_acquisitions'].split()]
    slave_acqs = [i.strip() for i in ctx['slave_acquisitions'].split()]
    logger.info("master_acqs : %s" %master_acqs)
    logger.info("slave_acqs : %s" %slave_acqs)
    
    spyddder_extract_version = ctx["spyddder_extract_version"]
    acquisition_localizer_version = ctx["acquisition_localizer_version"]
    standard_product_ifg_version = ctx["standard_product_ifg_version"]
    job_priority = ctx["job_priority"]
    job_type, job_version = ctx['context']['job_specification']['id'].split(':') 

    queues = []  # where should we get the queue value
    identifiers = []
    prod_dates = []
   

    acq_info = {}
    
    index_suffix = "S1-IW_ACQ"



    # Find out status of all Master ACQs, create a ACQ object with that and update acq_info dictionary
    for acq in master_acqs:
 	acq_type = "master"
	#logger.info(acq)
        #acq_data = util.get_acquisition_data(acq)[0]['fields']['partial'][0]
        acq_data = util.get_partial_grq_data(acq)['fields']['partial'][0]
        status = check_slc_status(acq_data['metadata']['identifier'])
        if status:
            # status=1
            logger.info("%s exists" %acq_data['metadata']['identifier'])
            acq_info[acq]=get_acq_object(acq, acq_type, acq_data, 1)
        else:
            #status = 0
            logger.info("%s does NOT exist"%acq_data['metadata']['identifier'])
            acq_info[acq]=get_acq_object(acq, acq_type, acq_data, 0)


    # Find out status of all Slave ACQs, create a ACQ object with that and update acq_info dictionary
    for acq in slave_acqs:
        #logger.info(acq)
	acq_type = "slave"
        #acq_data = util.get_acquisition_data(acq)[0]['fields']['partial'][0]
	#logger.info("ACQ value : %s" %acq)
	acq_data = util.get_partial_grq_data(acq)['fields']['partial'][0]
        status = check_slc_status(acq_data['metadata']['identifier'])
        if status:
	    # status=1
            logger.info("%s exists" %acq_data['metadata']['identifier'])
	    acq_info[acq]=get_acq_object(acq, acq_type, acq_data, 1)
        else:
	    #status = 0
	    logger.info("%s does NOT exist"%acq_data['metadata']['identifier'])
	    acq_info[acq]=get_acq_object(acq, acq_type, acq_data, 0)

    acq_infoes =[]
    projects = []
    job_priorities = []
    job_types = []
    job_versions = []
    spyddder_extract_versions = []
    acquisition_localizer_versions = []
    standard_product_ifg_versions = []

    acq_infoes.append(acq_info)
    projects.append(project)
    job_priorities.append(job_priority)
    job_types.append(job_type)
    job_versions.append(job_version)
    spyddder_extract_versions.append(spyddder_extract_version)
    acquisition_localizer_versions.append(acquisition_localizer_version)
    standard_product_ifg_versions.append(standard_product_ifg_version)

    #return acq_infoes, spyddder_extract_versions, acquisition_localizer_versions, standard_product_ifg_versions, projects, job_priorities, job_types, job_versions
    return acq_info, spyddder_extract_version, acquisition_localizer_version, standard_product_ifg_version, project, job_priority, job_type, job_version


def sling(acq_info, spyddder_extract_version, acquisition_localizer_version, standard_product_ifg_version, project, job_priority, job_type, job_version):
    '''
	This function checks if any ACQ that has not been ingested yet and sling them.
    '''
    #logger.info("acq_info type: %s : %s" %(type(acq_info), len(acq_info) ))
    #logger.info(acq_info)
    logger.info("%s : %s" %(type(spyddder_extract_version), spyddder_extract_version))
    # acq_info has now all the ACQ's status. Now submit the Sling job for the one's whose status = 0 and update the slc_info with job id
    for acq_id in acq_info.keys():

	if not acq_info[acq_id]['localized']:
	    acq_data = acq_info[acq_id]['acq_data']
	    job_id = submit_sling_job(project, spyddder_extract_version, acquisition_localizer_version, acq_data, job_priority)
 
	    acq_info[acq_id]['job_id'] = job_id
	    job_status, new_job_id  = get_job_status(job_id)
	    acq_info[acq_id]['job_id'] = new_job_id
	    acq_info[acq_id]['job_status'] = job_status


    # Now loop in until all the jobs are completed 
    all_done = False
    sling_check_start_time = datetime.utcnow()
    while not all_done:

        for acq_id in acq_info.keys():
	    acq_data = acq_info[acq_id]['acq_data']
            if not acq_info[acq_id]['localized']: 
		job_status, job_id  = get_job_status(acq_info[acq_id]['job_id'])  
  		if job_status == "job-completed":
		    logger.info("Success! sling job for slc : %  with job id : %s COMPLETED!!" %(acq_data['metadata']['identifier'], job_id))
		    acq_info[acq_id]['job_id'] = job_id
		    acq_info[acq_id]['job_status'] = job_status
		elif job_status == "job-failed":
		    download_url = acq_info[acq_id]['download_url']
           	    logger.info ("Job %s failed. So again smitting sling job for %s" %(acq_info[acq_id]['job_id'], download_url))
            	    job_id = submit_sling_job(project, spyddder_extract_version, acquisition_localizer_versions, acq_data, job_priority)
            	    acq_info[acq_id]['job_id'] = job_id
		    logger.info("New Job Id : %s" %acq_info[acq_id]['job_id'])
            	    job_status, new_job_id  = get_job_status(job_id)
            	    acq_info[acq_id]['job_id'] = new_job_id
            	    acq_info[acq_id]['job_status'] = job_status
		    logger.info("After checking job status, New Job Id : %s and status is %s" %(acq_info[acq_id]['job_id'], acq_info[acq_id]['job_status']))
		else:
		    acq_info[acq_id]['job_id'] = job_id
                    acq_info[acq_id]['job_status'] = job_status
		    logger.info("Sling job for %s  : Job id : %s. Job Status : %s" %(acq_info[acq_id], acq_info[acq_id]['job_id'], acq_info[acq_id]['job_status']))

  	logger.info("Checking if all job completed")
	all_done = check_all_job_completed(acq_info)
	if not all_done:
	    now = datetime.utcnow()
	    delta = (now - sling_check_start_time).total_seconds()
            if delta >= sling_completion_max_sec:
            	raise RuntimeError("Error : Sling jobs NOT completed after %.2f hours!!" %delta/3600)
	    logger.info("All job not completed. So sleeping for %s seconds" %sleep_seconds)
	    time.sleep(sleep_seconds)



    #At this point, all sling jobs have been completed. Now lets recheck the slc status


    logger.info("\nAll sling jobs have been completed. Now lets recheck the slc status")
    all_exists = False
    index_suffix = "S1-IW_ACQ"
    slc_check_start_time = datetime.utcnow()
    while not all_exists:
        all_exists = True
	for acq_id in acq_info.keys():
            if not acq_info[acq_id]['localized']:
		acq_data = acq_info[acq_id]['acq_data']
 		acq_info[acq_id]['localized'] = check_slc_status(acq_data['metadata']['identifier'])
		
		if not acq_info[acq_id]['localized']:
		    logger.info("%s NOT localized!!" %acq_data['metadata']['identifier'])
		    all_exists = False
		    break
	if not all_exists:
	    now = datetime.utcnow()
            delta = (now-slc_check_start_time).total_seconds()
	    if delta >= slc_check_max_sec:
                raise RuntimeError("Error : SLC not available %.2f min after sling jobs completed!!" %delta/60)
            time.sleep(60)


    # At this point, we have all the slc downloaded and we are ready to submit a create standard product job
    acq_infoes =[]
    projects = []
    job_priorities = []
    job_types = []
    job_versions = []
    standard_product_ifg_versions = []

    acq_infoes.append(acq_info)
    projects.append(project)
    job_priorities.append(job_priority)
    standard_product_ifg_versions.append(standard_product_ifg_version)
   

    return acq_infoes, projects, standard_product_ifg_versions, job_priorities 



        

def check_all_job_completed(acq_info):
    all_done = True
    for acq_id in acq_info.keys():
        if not acq_info[acq_id]['localized']:  
	    job_status = acq_info[acq_id]['job_status']
	    if not job_status == "job-completed":
		logger.info("check_all_job_completed : %s NOT completed!!" %acq_info[acq_id]['job_id'])	
		all_done = False
		break
    return all_done

def submit_ifg_job( acq_info, project, standard_product_ifg_version, job_priority, wuid=None, job_num=None):
    """Map function for create interferogram job json creation."""

    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")
    logger.info("\n\n\n SUBMIT IFG JOB!!!")
    master_ids_str=""
    master_ids_list=[]

    slave_ids_str=""
    slave_ids_list=[]


    logger.info("project : %s" %project)

    for acq in acq_info.keys():
	acq_data = acq_info[acq]['acq_data']
	acq_type = acq_info[acq]['acq_type']
	identifier =  acq_data["metadata"]["identifier"]
        logger.info("identifier : %s" %identifier)
	if acq_type == "master":
	    master_ids_list.append(identifier)
	    if master_ids_str=="":
		master_ids_str=identifier
	    else:
		master_ids_str += " "+identifier

	elif acq_type == "slave":
            slave_ids_list.append(identifier)
            if slave_ids_str=="":
                slave_ids_str=identifier
            else:
                slave_ids_str += " "+identifier


    logger.info("master_ids_str : %s" %master_ids_str)
    logger.info("slave_ids_str : %s" %slave_ids_str)
    # set job type and disk space reqs
    disk_usage = "300GB"

    # set job queue based on project
    job_queue = "%s-job_worker-large" % project
   
    job_type = "job-standard-product-ifg:%s" %standard_product_ifg_version

    job_hash = hashlib.md5(json.dumps([
	job_priority,
	master_ids_str,
	slave_ids_str
    ])).hexdigest()




    return {
        "job_name": "%s-%s" % (job_type, job_hash[0:4]),
        "job_type": "job:%s" % job_type,
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
            "master_ids": master_ids_str,
	    "slave_ids": slave_ids_str,
	    "job_priority" : job_priority,
	    "azimuth_looks" : 19,
	    "range_looks" : 7,
	    "filter_strength" : 0.5,
	    "precise_orbit_only" : "true",
	    "auto_bbox" : "true",

            # v2 cmd
            "_command": "/home/ops/ariamh/interferogram/sentinel/sciflo_create_standard_product.sh",

            # disk usage
            "_disk_usage": disk_usage,

        }
    }

def submit_sling_job(project, spyddder_extract_version, acquisition_localizer_versions, acq_data, priority):

    """Map function for spyddder-man extract job."""

    acquisition_localizer_version = "develop"

    job_submit_url = '%s/mozart/api/v0.1/job/submit' % MOZART_URL

    # set job type and disk space reqs
    job_type = "job-acquisition_localizer:{}".format(acquisition_localizer_versions)

     # set job type and disk space reqs
    disk_usage = "300GB"
    #logger.info(acq_data)
    #acq_id = acq_data['acq_id']

    # set job queue based on project
    job_queue = "%s-job_worker-large" % project

    rule = {
        "rule_name": "standard-product-sling",
        "queue": job_queue,
        "priority": '5',
        "kwargs":'{}'
    }

    sling_job_name = "standard_product-%s-%s" %(job_type, acq_data["metadata"]["identifier"])


    params = [
	{
            "name": "workflow",
            "from": "value",
            "value": "acquisition_localizer.sf.xml"
        },
        {
            "name": "project",
            "from": "value",
            "value": project
        },
        {
            "name": "spyddder_extract_version",
            "from": "value",
            "value": spyddder_extract_version
        },
        {
            "name": "dataset_type",
            "from": "value",
            "value": acq_data["dataset_type"]
        },
        {
            "name": "dataset",
            "from": "value",
            "value": acq_data["dataset"]
        },
        {
            "name": "identifier",
            "from": "value",
            "value": acq_data["metadata"]["identifier"]
        },
        {
            "name": "download_url",
            "from": "value",
            "value": acq_data["metadata"]["download_url"]
        },
        {
            "name": "archive_filename",
            "from": "value",
            "value": acq_data["metadata"]["archive_filename"]
        },
        {
            "name": "prod_met",
            "from": "value",
            "value": acq_data["metadata"]
        }
    ]
    

    logger.info("PARAMS : %s" %params)
    logger.info("RULE : %s"%rule)
    logger.info(job_type)
    logger.info(sling_job_name)

    mozart_job_id = submit_mozart_job({}, rule,hysdsio={"id": "internal-temporary-wiring", "params": params, "job-specification": job_type}, job_name=sling_job_name)
    logger.info("\nSubmitted sling job with id %s for  %s" %(acq_data["metadata"]["identifier"], mozart_job_id))

    return mozart_job_id
    

def main():
    #master_acqs = ["acquisition-S1A_IW_ACQ__1SDV_20180702T135953_20180702T140020_022616_027345_3578"]
    #slave_acqs = ["acquisition-S1B_IW_ACQ__1SDV_20180720T015751_20180720T015819_011888_015E1C_3C64"]
    master_acqs = ["acquisition-S1A_IW_ACQ__1SDV_20180807T135955_20180807T140022_023141_02837E_DA79"]
    slave_acqs =["acquisition-S1A_IW_ACQ__1SDV_20180714T140019_20180714T140046_022791_027880_AFD3", "acquisition-S1A_IW_ACQ__1SDV_20180714T135954_20180714T140021_022791_027880_D224", "acquisition-S1A_IW_ACQ__1SDV_20180714T135929_20180714T135956_022791_027880_9FCA"]


    #acq_data= util.get_partial_grq_data("acquisition-S1A_IW_ACQ__1SDV_20180702T135953_20180702T140020_022616_027345_3578")['fields']['partial'][0]
    acq_data= util.get_partial_grq_data("acquisition-S1A_IW_SLC__1SSV_20160630T135949_20160630T140017_011941_01266D_C62F")['fields']['partial'][0]
    print(acq_data) 
    
    #resolve_source(master_acqs, slave_acqs)
    print(acq_data["dataset_type"])
    print(acq_data["dataset"])    
    print(acq_data["metadata"]["identifier"]) 
    print(acq_data["metadata"]["download_url"])
    print(acq_data["metadata"]["archive_filename"])
    #print(acq_data["metadata"][""])
if __name__ == "__main__":
    main()





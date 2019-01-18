#!/usr/bin/env python

import os, sys, time, json, requests, logging, traceback

from hysds.celery import app

from standard_product_localizer import publish_data, get_acq_object


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


def resolve_acq(slc_id, version, es_url=app.conf['GRQ_ES_URL']):
    """Resolve acquisition id."""

    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"metadata.identifier.raw": slc_id}},
                    {"term": {"system_version.raw": version}},
                ]
            }
        },
        "fields": [],
    }
    logger.info("query: {}".format(json.dumps(query, indent=2)))
    es_index = "grq_{}_acquisition-s1-iw_slc".format(version)
    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))
    if r.status_code != 200:
        logger.error("Failed to query %s:\n%s" % (es_url, r.text))
        logger.error("query: %s" % json.dumps(query, indent=2))
        logger.error("returned: %s" % r.text)
        r.raise_for_status()
    result = r.json()
    logger.info("result: {}".format(json.dumps(result, indent=2)))

    if len(result['hits']['hits']) == 0:
        raise ValueError("Couldn't find record with ID: %s, at ES: %s"%(slc_id, es_url))

    return result['hits']['hits'][0]['_id']


def all_slcs_exist(acq_ids, acq_version, slc_version, es_url=app.conf['GRQ_ES_URL']):
    """Check that SLCs exist for the acquisitions."""

    query = {
        "query": {
            "ids": {
                "values": acq_ids,
            }
        },
        "fields": [
          "metadata.identifier"
        ]
    }
    logger.info("query: {}".format(json.dumps(query, indent=2)))
    acq_index = "grq_{}_acquisition-s1-iw_slc".format(acq_version)
    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, acq_index)
    else:
        search_url = '%s/%s/_search' % (es_url, acq_index)
    r = requests.post(search_url, data=json.dumps(query))
    if r.status_code != 200:
        logger.error("Failed to query %s:\n%s" % (es_url, r.text))
        logger.error("query: %s" % json.dumps(query, indent=2))
        logger.error("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    logger.info("result: {}".format(json.dumps(result, indent=2)))

    # extract slc ids
    slc_ids = []
    if result['hits']['total'] > 0:
        for hit in result['hits']['hits']:
            slc_ids.append(hit['fields']['metadata.identifier'][0])
    logger.info("slc_ids: {}".format(slc_ids))
    if len(slc_ids) != len(acq_ids):
        raise RuntimeError("Failed to resolve SLC IDs for all acquisition IDs: {} vs. {}".format(acq_ids, slc_ids))

    # check all slc ids exist
    query = {
        "query": {
            "ids": {
                "values": slc_ids,
            }
        },
        "fields": []
    }
    logger.info("query: {}".format(json.dumps(query, indent=2)))
    slc_index = "grq_{}_s1-iw_slc".format(slc_version)
    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, slc_index)
    else:
        search_url = '%s/%s/_search' % (es_url, slc_index)

    r = requests.post(search_url, data=json.dumps(query))
    if r.status_code != 200:
        logger.error("Failed to query %s:\n%s" % (es_url, r.text))
        logger.error("query: %s" % json.dumps(query, indent=2))
        logger.error("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    logger.info("result: {}".format(json.dumps(result, indent=2)))
        
    # extract slc ids that exist
    existing_slc_ids = []
    if result['hits']['total'] > 0:
        for hit in result['hits']['hits']:
            existing_slc_ids.append(hit['_id'])
    logger.info("existing_slc_ids: {}".format(existing_slc_ids))
    if len(slc_ids) != len(existing_slc_ids):
        logger.info("Missing SLC IDs: {}".format(list(set(slc_ids) - set(existing_slc_ids))))
        return False
    return True


def get_acqlists_by_acqid(acq_id, acqlist_version, es_url=app.conf['GRQ_ES_URL']):
    """Return all acq-list datasets that contain the acquisition ID."""

    query = {
        "query": {
            "bool": {
                "must": [
                    { "term": {"system_version.raw": acqlist_version}},
                    {
                        "bool": {
                            "should": [
                                {
                                    "term": {
                                        "metadata.master_scenes.raw": acq_id
                                    }
                                },
                                {
                                    "term": {
                                        "metadata.slave_scenes.raw": acq_id
                                    }
                                }
                            ]
                        }
                    },
                ]
            }
        },
        "partial_fields" : {
            "partial" : {
                "exclude" : ["city", "context", "continent"],
            }
        }
    }
    logger.info("query: {}".format(json.dumps(query, indent=2)))
    es_index = "grq_{}_acq-list".format(acqlist_version)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)

    r = requests.post(search_url, data=json.dumps(query))
    if r.status_code != 200:
        logger.error("Failed to query %s:\n%s" % (es_url, r.text))
        logger.error("query: %s" % json.dumps(query, indent=2))
        logger.error("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    logger.info("result: {}".format(json.dumps(result, indent=2)))

    if len(result['hits']['hits']) == 0:
        raise ValueError("Couldn't find record with ID: %s, at ES: %s"%(acq_id, es_url))

    return [i['fields']['partial'][0] for i in result['hits']['hits']]


def main():
    """Main."""

    # read in context
    context_file = os.path.abspath("_context.json")
    if not os.path.exists(context_file):
        raise(RuntimeError("Context file doesn't exist."))
    with open(context_file) as f:
        ctx = json.load(f)
    
    # resolve acquisition id from slc id
    slc_id = ctx['slc_id']
    slc_version = ctx['slc_version']
    acq_version = ctx['acquisition_version']
    acq_id = resolve_acq(slc_id, acq_version)
    logger.info("acq_id: {}".format(acq_id))

    # pull all acq-list datasets with acquisition id in either master or slave list
    acqlist_version = ctx['acqlist_version']
    acqlists = get_acqlists_by_acqid(acq_id, acqlist_version)
    logger.info("Found {} matching acq-list datasets".format(len(acqlists)))
    for acqlist in acqlists:
        logger.info(json.dumps(acqlist, indent=2))
        acq_info = {}
        for acq in acqlist['metadata']['master_scenes']:
            acq_info[acq]=get_acq_object(acq, "master")
        for acq in acqlist['metadata']['slave_scenes']:
    	    acq_info[acq]=get_acq_object(acq, "slave")
        if all_slcs_exist(acq_info.keys(), acq_version, slc_version):
            publish_data(acq_info, acqlist['metadata']['project'], acqlist['metadata']['job_priority'],
                         acqlist['metadata']['dem_type'], acqlist['metadata']['track'], 
                         acqlist['metadata']['starttime'], acqlist['metadata']['endtime'],
                         acqlist['metadata']['master_scenes'], acqlist['metadata']['slave_scenes'],
                         acqlist['metadata']['orbitNumber'], acqlist['metadata']['direction'],
                         acqlist['metadata']['platform'], acqlist['metadata']['union_geojson'],
                         acqlist['metadata']['bbox'], acqlist['metadata']['list_master_dt'],
                         acqlist['metadata']['list_slave_dt'])


if __name__ == "__main__":
    try: status = main()
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
    sys.exit(status)

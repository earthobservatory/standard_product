#!/usr/bin/env python 
import os, sys, time, json, requests, logging, re

def get_orbit_from_orbit_file(orbit_file):
    print("get_orbit_from_orbit_file : {}".format(orbit_file))
    es_url = "http://128.149.127.152:9200"
    es_index = "grq"
    query = {
       "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "metadata.archive_filename.raw": orbit_file
                         }
                    }
                ]
            }
        }
    }

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

    if len(result["hits"]["hits"]) == 0:
        raise ValueError("Couldn't find record with orbit file: %s, at ES: %s"%(orbit_file, es_url))
        return

    #LOGGER.debug("Got: {0}".format(json.dumps(result)))
    h = result["hits"]["hits"][0]
    fields = h['_source']
    prod_url = fields['urls'][0]
    if len(fields['urls']) > 1:
        for u in fields['urls']:
            if u.startswith('http://'):
                prod_url = u
                break

    orbit_url = os.path.join(prod_url, orbit_file)
    print("get_orbit_from_orbit_file : orbit_url : {}".format(orbit_url))
    return orbit_url

    


orbit_file = "S1A_OPER_AUX_POEORB_OPOD_20180613T120753_V20180523T225942_20180525T005942.EOF"
print(get_orbit_from_orbit_file(orbit_file))

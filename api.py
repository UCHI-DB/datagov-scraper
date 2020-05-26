import requests
import pprint
import csv

import config as C
import ckan as CK
from workerpool import workerpool as P

# The base query to list datasets
list_dataset_query = C.datagovbaseurl + CK.package

# Datasets available
total_num_datasets = -1

def paginate_over_all_results(total_num_datasets, callback, *args):
    '''
      Paginates over all results given by CKAN. Applies a callback
      function (with args parameter) to those results until the 
      total_num_datasets is reached.
    '''
    start = 0
    num_datasets = 0
    finished = False
    while not finished:
        print("Processing... " + \
            str(start) + "/" + \
            str(total_num_datasets)
        )
        params = {"rows": C.maxrows, "start":start}
        data = query_datasets_ckan(params)
        results = data["results"]
        num_results = len(results)
        if num_results > 0:
            callback(results, *args)
        else:
            finished = True
        start = start + num_results
        num_datasets = num_datasets + num_results
        if num_datasets >= total_num_datasets:
            finished  = True
    print("DONE pagination!")
    
def query_datasets_ckan(params):
    '''
    Retrieves the list of datasets available in data.gov
    '''
    raw_resp = requests.get(list_dataset_query, params)
    json_res = None
    try:
        json_res = raw_resp.json()
    except ValueError:
        print("While trying to decode JSON")
        exit()
    print("SUCCESS")
    success = json_res["success"]
    if success:
        return json_res["result"]
    else:
        print("ERROR: while trying to parse response")
        exit()

def get_total_num_datasets():
    '''
    Gets the total number of datasets available in data.gov
    '''
    global total_num_datasets
    if total_num_datasets is -1:
        # retrieve number from ckan
        json_res = query_datasets_ckan({})
        total_num_datasets = json_res["count"]
    print("total_num_datasets: " + str(total_num_datasets))
    return total_num_datasets

def extract_urls_from_datagov_result(r):
    '''
    Given the results returned by data.gov, it parses them
    to extract the URLs of a given type.
    '''
    rows = []
    dataset_name = r["name"]
    dataset_id = r["id"]
    download_resources = r["resources"]
    for res in download_resources:
        dataset_f = res["format"]
        #if f in C.ok_formats:
        row = [dataset_name, dataset_id, dataset_f, res["url"]]
        rows.append(row)
    return rows 

def dump_urls_to_csvfile(results, csvwriter):
    '''
    Writes URLs to a CSV file
    '''
    for r in results:
        rows = extract_urls_from_datagov_result(r)
        for row in rows:
            csvwriter.writerow(row)

def classify_unique_urls_2(csvfile):
    '''
    Creates a dict where it shows the number of datasets (value)
    that can be downloaded *only* on a specific format (key)
    '''
    included = dict()
    url_included = dict()
    classification = dict()
    classification["CSV"] = []
    classification["XML"] = []
    classification["PDF"] = []
    classification["HTML"] = []
    classification["JSON"] = []
    classification["other"] = []
    num_urls = 0 
    non_repeated_url = 0
    unique_datasets = dict()
    with open(csvfile, 'r') as f:
        csvreader = csv.reader(f, delimiter=",")
        local_aggr_dataset = []
        last_dname = None
        for (dname, did, dformat, durl) in csvreader:
            if dname not in unique_datasets:
                unique_datasets[dname] = []
            unique_datasets[dname].append((dname, did, dformat, durl))
    total_unique_datasets = len(unique_datasets)
    for k,v in unique_datasets.items():
        # group all urls for a given dataset per format
        local_aggr = dict()
        local_aggr["other"] = []
        for (dname, did, dformat, durl) in v:
            if dformat not in local_aggr:
                local_aggr[dformat] = []                    
            local_aggr[dformat].append(durl)
        # find in priority order the desired formats or put URL in other
        if "CSV" in local_aggr:
            get_url = local_aggr["CSV"][0] # any of the non repeated
            classification["CSV"].append(get_url) 
        elif "XML" in local_aggr:
            get_url = local_aggr["XML"][0] # any of the non repeated
            classification["XML"].append(get_url) 
        elif "PDF" in local_aggr:
            get_url = local_aggr["PDF"][0] # any of the non repeated
            classification["PDF"].append(get_url) 
        elif "HTML" in local_aggr:
            get_url = local_aggr["HTML"][0] # any of the non repeated
            classification["HTML"].append(get_url) 
        elif "JSON" in local_aggr:
            get_url = local_aggr["JSON"][0] # any of the non repeated
            classification["JSON"].append(get_url) 
        else:
            classification["other"].append("other.com") 
    
    total_unique_urls = 0
    for k,v in classification.items():
        total_unique_urls = total_unique_urls + len(v) 

    print("STATISTICS")
    print("Total unique datasets: " +str(total_unique_datasets))
    print("Total unique files: " + str(total_unique_urls))
    print("---------")
    print("Classification by file type")
    unordered = []
    for key, value in classification.items():
        unordered.append((key, len(value)))
    ordered = sorted(unordered, key=lambda x: x[1] )
    for k,v in ordered:
        print("K: " + str(k) + " V: " + str(v))



def classify_unique_urls(csvfile):
    '''
    Creates a dict where it shows the number of datasets (value)
    that can be downloaded *only* on a specific format (key)
    '''
    included = dict()
    url_included = dict()
    classification = dict()
    classification["CSV"] = []
    classification["XML"] = []
    classification["PDF"] = []
    classification["HTML"] = []
    classification["JSON"] = []
    classification["other"] = []
    num_urls = 0 
    non_repeated_url = 0
    with open(csvfile, 'r') as f:
        csvreader = csv.reader(f, delimiter=",")
        local_aggr_dataset = []
        last_dname = None
        for (dname, did, dformat, durl) in csvreader:
            if dname is not last_dname: # Process previous dataset group
                # group by format
                local_aggr = dict()
                local_aggr["other"] = []
                for (ldname, ldid, ldformat, ldurl) in local_aggr_dataset:
                    if ldformat not in local_aggr:
                        local_aggr[ldformat] = []                    
                    local_aggr[ldformat].append(ldurl)
                # include only one type of format out of the preferred ones or
                # other
                if "CSV" in local_aggr:
                    get_url = local_aggr["CSV"][0] # any of the non repeated
                    classification["CSV"].append(get_url) 
                elif "XML" in local_aggr:
                    get_url = local_aggr["XML"][0] # any of the non repeated
                    classification["XML"].append(get_url) 
                elif "PDF" in local_aggr:
                    get_url = local_aggr["PDF"][0] # any of the non repeated
                    classification["PDF"].append(get_url) 
                elif "HTML" in local_aggr:
                    get_url = local_aggr["HTML"][0] # any of the non repeated
                    classification["HTML"].append(get_url) 
                elif "JSON" in local_aggr:
                    get_url = local_aggr["JSON"][0] # any of the non repeated
                    classification["JSON"].append(get_url) 
                else:
                    #get_url = local_aggr["other"][0] # any of the non repeated
                    classification["other"].append("other.com") 
                # clean up variables for next dataset group
                local_aggr_dataset = []
            else:
                local_aggr_dataset.append((dname, did, dformat, durl))
            last_dname = dname
                
    total_unique_urls = 0
    for k,v in classification.items():
        total_unique_urls = total_unique_urls + len(v) 

    print("STATISTICS")
    print("Total unique files: " + str(total_unique_urls))
    print("---------")
    print("Classification by file type")
    unordered = []
    for key, value in classification.items():
        unordered.append((key, len(value)))
    ordered = sorted(unordered, key=lambda x: x[1] )
    for k,v in ordered:
        print("K: " + str(k) + " V: " + str(v))


def classify_urls(csvfile):
    '''
    Creates a dict where types of file formats are counted
    '''
    included = dict()
    classification = dict()
    num_urls = 0 
    non_repeated_url = 0
    with open(csvfile, 'r') as f:
        csvreader = csv.reader(f, delimiter=",")
        for (dname, did, dformat, durl) in csvreader:
            num_urls = num_urls + 1
            if durl not in included:
                non_repeated_url = non_repeated_url + 1
                if dformat not in classification:
                    classification[dformat] = []
                classification[dformat].append(durl)
                included[durl] = 1
    print("STATISTICS")
    print("Total files: " + str(num_urls))
    print("Total unique files: " + str(non_repeated_url))
    print("---------")
    print("Classification by file type")
    unordered = []
    for key, value in classification.items():
        unordered.append((key, len(value)))
    ordered = sorted(unordered, key=lambda x: x[1] )
    for k,v in ordered:
        print("K: " + str(k) + " V: " + str(v))


def download_dataset(file_descriptor, target_path):
    print("Download dataset: " + str(file_descriptor))
    print("into path: " + str(target_path))
    # start pool of workers
    P.populate_q_with_work(file_descriptor)
    P.start_workers(target_path)


if __name__ == "__main__":
    print("Scraper API")

    # Get total number of datasets
    #datasets = get_total_num_datasets()

    # Learn about the format of each dataset
    #params = {"rows": 1000, "start":0}
    #data = query_datasets_ckan(params)
    #results = data["results"]
    #pprint.pprint(results)
    
    #total_num_datasets = 189000
    #paginate_over_all_results(total_num_datasets)
    
    filename = "alldatagovurls.csv"
    # test
    #total_num_datasets = 1

    #with open(filename, "a+") as f:
    #    csvw = csv.writer(f, delimiter=',')
    #    paginate_over_all_results(total_num_datasets, dump_urls_to_csvfile, csvw)

    #path = "data/"
    #download_dataset(filename, path)

    # statistics about data urls
    classify_unique_urls_2(filename)
    

import json
import argparse
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime, timedelta


basePath = "/Users/swapnilshashank/IdeaProjects/BigQueryExecution/venv/big/query/execution/sql/queries"
global project_path

def readConfig(project_path):
    configFile = project_path + "/config.json"
    f = open(configFile)
    json_file = json.load(f)
    #print(json_file)
    for item in json_file['query_details']:
        query_id = item['query_id']
        dataset_id = item['dataset_id']
        table_id = item['table_id']
        print(query_id, dataset_id, table_id)
        constructQuery(query_id, dataset_id, table_id)

def constructQuery(query_id, dataset_id, table_id):
    ddlFilePath = project_path + "/DDL" + query_id + ".sql"
    ddlFile = open(file=ddlFilePath,mode='r').read()
    print(project_id)
    print(dataset_id)
    print(table_id)
    ddlStmt = ddlFile.format(project_id, dataset_id, table_id)
    print(ddlStmt)
    queryFilePath = project_path + "/Query" + query_id + ".sql"
    queryStmt = open(file=queryFilePath,mode='r').read()
    print(queryStmt)
    executeQueryOnBQ(ddlStmt)
    executeQueryOnBQ(queryStmt)


def executeQueryOnBQ(query):
    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    job_config.allow_large_results = True
    create_table_query_job = client.query(query, job_config=job_config)
    create_table_query_job.result()
    print("Query executed successfully!")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project_id', dest='project_id', type=str, help='Add project_id')
    args = parser.parse_args()

    print (args.project_id)
    global project_id, project_path
    project_id = args.project_id
    project_path = basePath + "/" + project_id
    readConfig(project_path)

if __name__ == "__main__":
    main()


import boto3
import time
import json
import sys

# Defaults
query_output = 's3://platform-prd-my-athena-output-bucket/outputs'
queryparams = {}
queryparams['execution_id']=''
athena = boto3.client('athena')

# functions
# queryparams is mutable, so that execution_id has to be returned to the caller for further processing

def run_athena_query (query, queryparams):
    print "Executing query:\n{0}".format(query)
    response = athena.start_query_execution(
        QueryString=ddl_query,
        ResultConfiguration={
            'OutputLocation': query_output
        }
    )
    execution_id = response['QueryExecutionId']
    queryparams['execution_id'] = execution_id
    status = ''
    while True:
        stats = athena.get_query_execution(QueryExecutionId=execution_id)
        status = stats['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            return status
        time.sleep(0.2)  # 200ms

# Print the results of the query execution
def print_results(execution_id):
    results = athena.get_query_results(QueryExecutionId=execution_id)
    print (json.dumps(results, sort_keys=True, indent=4))

def main():
    query = '''
        CREATE TABLE IF NOT EXISTS amplitude_feed (
            $schema               LONG,
            adid                  STRING,
            amplitude_attribution_ids STRING,
            amplitude_event_type  STRING,
            amplitude_id          LONG,
            city                  STRING,
            client_event_time     STRING,
            client_upload_time    STRING,
            country               STRING,
            data STRUCT <first_event:BOOLEAN>,
            device_brand          STRING,
            device_carrier        STRING,
            device_family         STRING,
            device_manufacturer   STRING,
            device_model          STRING,
            device_type           STRING,
            dma                   STRING,
            event_id              LONG,
            event_time            STRING,
            event_type            STRING,
            idfa                  STRING,
            is_attribution_event  BOOLEAN,
            language              STRING,
            library               STRING,
            location_lat          STRING,
            location_lng          STRING,
            os_name               STRING,
            os_version            STRING,
            paying                STRING,
            platform              STRING,
            processed_time        STRING,
            region                STRING,
            sample_rate           STRING,
            server_received_time  STRING,
            server_upload_time    STRING,
            start_version         STRING,
            user_creation_time    STRING,
            version_name          STRING
        )
        PARTITIONED BY (year STRING, month STRING, day STRING)
        STORED AS PARQUET
        LOCATION 's3://tup-redshift/test_dummy/'
        tblproperties ("parquet.compress"="SNAPPY");
        ;
    '''
    
    queryparams['execution_id']=''
    
    ret = run_athena_query(query, queryparams)
    if ret !=  'SUCCEEDED' :
        print ret
        sys.exit(1)
    print_results(queryparams['execution_id'])


if __name__ == "__main__":
    main()

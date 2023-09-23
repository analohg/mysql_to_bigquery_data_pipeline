#import all modules
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from google.cloud import storage
from google.cloud import bigquery
import pymysql
import os
import pyarrow


def handler(request):
    #ssh variables
    ssh_host = 'enter ssh host'
    ssh_port = 'enter ssh port'
    ssh_user = 'enter ssh user'
    localhost = 'enter localhost'

    #set the ssh_private_key_path to your private key file to enable key-based authentication
    cd = os.getcwd()
    file = 'enter key filename'
    ssh_private_key_path = os.path.join(cd, file)
  


    #database variables
    db_user = 'enter db username'
    db_password = 'enter db password'
    db_name = 'enter database name'
    db_port = 'enter db_port'
    db_host = localhost
    
    
    df = None

    #ssh tunnel details
    def query():
        nonlocal df
        with SSHTunnelForwarder(
                (ssh_host, ssh_port),
                ssh_username=ssh_user,
                ssh_pkey = ssh_private_key_path ,
                remote_bind_address=(db_host, db_port),
                ) as tunnel:
                    connection = pymysql.connect(
                        host=localhost, 
                        port=tunnel.local_bind_port,
                        user=db_user,
                        password=db_password,
                        database=db_name,
                        charset='utf8mb4',
                        cursorclass=pymysql.cursors.DictCursor,
                    )

                    try:
                        # Use the connection object for database operations
                        with connection.cursor() as cursor:
                            # Execute SQL queries
                            cursor.execute("SELECT * FROM table_1")
                            result = cursor.fetchall()
                            df = pd.DataFrame(result)
                    finally:
                        connection.close()

    # call the query function            
    query()
    
    cd = os.getcwd()
    file = 'enter json file with key'
    file_path = os.path.join(cd, file)
    
    #set the google application credentials to the file_path variable
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = file_path

    dataset_id = 'enter dataset id'
    project_id = 'enter project_id'
    table_name = 'enter table name'

    client = bigquery.Client()

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_name)
        
    job_config = bigquery.LoadJobConfig(
        #create the table schema 
        schema = [
                bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("email", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("phone_number", bigquery.enums.SqlTypeNames.INTEGER),
                ],
            autodetect = False,
            write_disposition = "WRITE_TRUNCATE",
            source_format=bigquery.SourceFormat.CSV,
            max_bad_records = 10
        )
    
    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)


    load_job.result()

    table = client.get_table(table_ref)
            
    print("Starting job {}".format(load_job))
    print(
            "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_ref
        ))
    return ("Done!", 200)

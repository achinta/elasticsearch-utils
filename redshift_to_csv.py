import boto3
import sys
import os
import psycopg2

REDSHIFT_HOST = "ivy-dev.chwrf3ujkohq.ap-southeast-1.redshift.amazonaws.com"
BUCKET_NAME = "ivyetl-dev"

unload_stmt = "unload ('SELECT * FROM {0}') to '{1}' credentials '{2}' allowoverwrite parallel off"

aws_access_key = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_key = os.environ['AWS_SECRET_ACCESS_KEY']
aws_region = 	 os.environ['AWS_DEFAULT_REGION']
s3_access_credentials = "aws_access_key_id=%s;aws_secret_access_key=%s" % (aws_access_key, aws_secret_key)
client = boto3.client('s3')

tables = ['dim_product','fct_invoice']


def conn_to_rs(host, db, usr, pwd):
	conn_str =  "dbname='{0}' host='{1}' port='5439' user='{2}' password='{3}'".format(db,host,usr,pwd)
	# print "conn_str: ", conn_str
	rs_conn = psycopg2.connect(conn_str) 
	return rs_conn

def unload_data(conn, s3_access_credentials, dataStagingPath, table_name):
	print "Exporting %s to %s" % (table_name, dataStagingPath)
	cursor = conn.cursor()
	query = unload_stmt.format(table_name, dataStagingPath, s3_access_credentials)
	# print 'unload query: ', query
	cursor.execute(query)

def copy_to_local(s3_key, local_path):
	print "copy to local with prefix: ",s3_key
	files = client.list_objects_v2(Bucket=BUCKET_NAME,Prefix=s3_key)
	for f in files['Contents']:
		with open(local_path,'wb') as local_file:
			client.download_fileobj(BUCKET_NAME,f['Key'], local_file)

if __name__ == '__main__':
	#delete the existing files in the unload folder
	files = client.list_objects_v2(Bucket=BUCKET_NAME,Prefix='unload')
	for f in files['Contents']:
		print 'deleting in S3 before download: ',f['Key']
		client.delete_object(Bucket=BUCKET_NAME,Key=f['Key'])

	src_conn = conn_to_rs(REDSHIFT_HOST,'dms_india_qa_reports_test','admin','Ivy12345')
	for table in tables:
		staging_path = "s3://" + BUCKET_NAME + "/unload/" + table + '.csv' 
		unload_data(src_conn,s3_access_credentials,staging_path,table)
		copy_to_local("unload/" + table + '.csv','/tmp/'+table+'.csv')







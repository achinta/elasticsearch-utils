import csv
import sys
from elasticsearch import Elasticsearch, helpers
import datetime, time
import argparse
# import codecs

class ElasticImporter():
	def __init__(self,elastic_url=None, batch_size=10):
		self.elastic_url = elastic_url
		self.batch_size = batch_size
		self.es = Elasticsearch()
		reload(sys)
		sys.setdefaultencoding('utf-8')
		
	def import_file(self,file_path, index, doc_type, id_column):
		start_time = time.time()
		with open(file_path, 'rb') as csvfile:
			reader = csv.reader(csvfile,quotechar='"')
			batch_count = 0
			rows = []
			for idx, row in enumerate(reader):
				if idx == 0:
					header = row
					continue
				
				rows.append(row)
				if len(rows) == self.batch_size:
					self.import_to_es(header, rows, index, doc_type,id_column)
					rows = []
				
				if idx%1000 == 0: 
					print '{0} secs. imported {1} rows for index {2}'.format((time.time() - start_time),idx, index)
			print '{0} secs. imported {1} rows for index {2}. Done.'.format((time.time() - start_time),idx, index)
					

	def import_to_es(self, header, rows, index, doc_type, id_column):
		actions = []
		for row in rows:
			action = dict(zip(header, row))
			action['_index'] = index
			action['_type'] = doc_type
			action['_id'] = action.pop(id_column)
			actions.append(action)
		helpers.bulk(self.es, actions)
		
if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-filepath')
	parser.add_argument('-index')
	parser.add_argument('-doctype')
	parser.add_argument('-idcolumn')
	args = parser.parse_args()

	importer = ElasticImporter()
	importer.import_file(args.filepath,args.index, args.doctype, args.idcolumn)


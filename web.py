import os

import logging
logging.basicConfig(filename="/data/logs/uuid-generator-logs.txt",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)

from flask import request
from string import Template
from helpers import query, generate_uuid
from escape_helpers import *


@app.route("/hello")
def hello():
	return "Hello from the mu-python-template!"


@app.route("/list-classes")
def listClasses():
	try:
		query_string = """
			select distinct ?o {
				graph <http://mu.semte.ch/graphs/public> {
					?s a ?o .
				}
			}"""	
		query_result = query(query_string)['results']['bindings']
		classes = [ i['o']['value'] for i in query_result ]
		return {
			"classes": classes
		}
	
	except Exception as e:
		logging.exception(e)
		raise e


@app.route("/generate-uuids", methods = [ 'POST' ])
def generateUuids():
	# helper function to split a list in to multiple batches
	def split(list_a, chunk_size):
		for i in range(0, len(list_a), chunk_size):
			yield list_a[i:i + chunk_size]

	body = request.get_json()
	className = body.get('className')
	if className is None: return (
		{ "message": "Mandatory field 'className' is missing" }, 
		400 
	)

	# Get all the instances of a class
	instances = queryInstances(className)

	# size of each batch to write to the triplestore
	BATCH_SIZE = 1000 
	for batch in split(instances, BATCH_SIZE):
		logging.info(f"STARTING BATCH: {len(batch)}")
		logging.info(f"TOTAL: {len(instances)}")
	       
		# Generate uuids for them
		batchWithIDs = ""
		for i in batch:
			triple = f"<{i}> <http://mu.semte.ch/vocabularies/core/uuid> {sparql_escape_string(generate_uuid())}"
			batchWithIDs += f"{triple} . "

		# Write it to triplestore
		try:
			query_string = Template("""
				DELETE {
					GRAPH <http://mu.semte.ch/graphs/public> {
						?s <http://mu.semte.ch/vocabularies/core/uuid> ?o . 	
					}		
				}
				INSERT { 
					GRAPH <http://mu.semte.ch/graphs/public> {
						 $batchWithIDs
					}
				}"""
			).substitute(batchWithIDs=batchWithIDs)

			query_result = query(query_string)
			logging.info(f"INFO: {query_result}")
		
		except Exception as e:
			logging.exception(f"EXCEPTION: {e}")
			raise e

	return {
		"result": query_result
	}


@app.route("/clear-uuids")
def clearUuids():
	body = request.get_json()
	className = body.get('className')
	if className is None: return (
		{ "message": "Mandatory field 'className' is missing" }, 
		400 
	)

	try:
		query_string = Template("""
			DELETE { GRAPH <http://mu.semte.ch/graphs/public> {
				?s <http://mu.semte.ch/vocabularies/core/uuid> ?o .
			}}
			WHERE { GRAPH <http://mu.semte.ch/graphs/public> {
				?s a $className ;
					<http://mu.semte.ch/vocabularies/core/uuid> ?o .
			}}"""
		).substitute(className=sparql_escape_uri(className))
		
		query_result = query(query_string)['results']
		logging.info(query_result )
	
	except Exception as e:
		logging.exception(e)
		raise e
	
	return {
		"result": query_result
	}


def queryInstances(className):
	try:
		query_string = Template("""
			select distinct ?s {
				graph <http://mu.semte.ch/graphs/public> {
					?s a $className .
				}
			}"""
		).substitute(
			className=sparql_escape_uri(className)
		)
		query_result = query(query_string)['results']['bindings']
		instances = [ i['s']['value'] for i in query_result ]
		return instances

	except Exception as e:
		logging.exception(e)
		raise e
	


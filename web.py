import os
import logging
logging.basicConfig(level = os.environ.get("LOGLEVEL", "INFO"))

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
		logging.info(classes)
		return {
			"classes": classes
		}
	
	except Exception as e:
		logging.exception(e)
		raise e


@app.route("/generate-uuids")
def generateUuids():

	className = "http://data.vlaanderen.be/ns/besluit#Agendapunt"

	# Get all the instances of a class
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

	except Exception as e:
		logging.exception(e)
		raise e

	# Generate uuids for them
	instancesWithIDs = ""
	for i in instances:
		triple = f"<{i}> <http://mu.semte.ch/vocabularies/core/uuid> \"{generate_uuid()}\""
		instancesWithIDs += f"{triple} . "

	# Write it to triplestore
	try:
		query_string = Template("""
			INSERT DATA { GRAPH <http://mu.semte.ch/graphs/public> {
				$instancesWithIDs
			}}"""
		).substitute(instancesWithIDs=instancesWithIDs)
		
		logging.info(query_string)
		query_result = query(query_string)['results']
		logging.info(query_result)
		return {
			"result": "soup"
		}
	
	except Exception as e:
		logging.exception(e)
		raise e


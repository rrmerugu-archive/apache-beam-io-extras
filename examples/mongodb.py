#!/usr/bin/env python
import sys

sys.path.append("../")
import logging
from beam_io_extras.mongo import ReadFromMongo
import apache_beam as beam


def transform_doc(document):
    return {'_id': str(document['_id'])}


DATAFLOW_RUNNER = "DirectRunner"

MONGODB_HOST = "127.0.0.1"
MONGODB_DATABASE_NAME = "test_db"
MONGODB_USERNAME = ""
MONGODB_PASSWORD = ""
COLLECTION_NAME = "test_collection"

connection_string = "mongodb://%s/%s" % (MONGODB_HOST, MONGODB_DATABASE_NAME)


def run():
    logging.basicConfig(level=logging.DEBUG)
    with beam.Pipeline(runner=DATAFLOW_RUNNER) as pipeline:
        (pipeline
         | 'read' >> ReadFromMongo(connection_string, MONGODB_DATABASE_NAME, 'knowledge', query={}, fields=['_id'])
         | 'transform' >> beam.Map(transform_doc)
         | 'save' >> beam.io.WriteToText('./documents.txt'))


if __name__ == '__main__':
    run()

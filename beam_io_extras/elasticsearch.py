from __future__ import absolute_import
from apache_beam.transforms import DoFn
from apache_beam.transforms import ParDo
from apache_beam.transforms import PTransform
from elasticsearch.helpers import bulk


class ElasticSearchWriteFn(DoFn):
    """
    ## Needed :
    pip install elasticsearch


    ## Usage:
    from elasticsearch import Elasticsearch
    es_client = Elasticsearch(hosts=["127.0.0.1:9200"], timeout=5000)


    """

    def __init__(self,
                 es_client=None,
                 index_name=None,
                 doc_type=None,
                 batch_size=None,
                 test_client=None,
                 mapping=None):
        self.es_client = es_client
        self.index_name = index_name
        self.doc_type = doc_type
        self.mapping = mapping

        self.batch_size = batch_size
        self.test_client = test_client
        self._max_batch_size = batch_size or 500

    def _pre_process_element(self, element):
        """
        Over ride this to extend
        :param element:
        :return:
        """
        return element

    def start_bundle(self):
        self._rows_buffer = []

    def process(self, element, unused_create_fn_output=None):
        self._rows_buffer.append(self._pre_process_element(element))
        if len(self._rows_buffer) >= self._max_batch_size:
            self._flush_batch()

    def finish_bundle(self):
        if self._rows_buffer:
            self._flush_batch()
        self._rows_buffer = []

    def _flush_batch(self):
        self.es_client.add(self._rows_buffer)
        success, _ = bulk(self.es_client,
                          self._rows_buffer,
                          index=self.index_name,
                          doc_type=self.doc_type,
                          raise_on_error=True) # TODO - check if raise_on_error is needed
        if success:
            print ("inserted {} docs into {}/{} ".format(len(self._rows_buffer), self.index_name, self.doc_type))
        self._rows_buffer = []


class WriteToElasticSearch(PTransform):

    def __init__(self,
                 es_client=None,
                 index_name=None,
                 doc_type=None,
                 batch_size=None,
                 test_client=None,
                 mapping=None):
        """
        Initialize a WriteToElasticSearch transform.

        """
        self.es_client = es_client
        self.index_name = index_name
        self.doc_type = doc_type
        self.mapping = mapping

        self.batch_size = batch_size
        self.test_client = test_client

    def expand(self, pcoll):
        mongodb_write_fn = ElasticSearchWriteFn(
            es_client=self.es_client,
            index_name=self.index_name,
            doc_type=self.doc_type,
            test_client=self.test_client,
            mapping=self.mapping
        )
        return pcoll | 'WriteToElasticSearch' >> ParDo(mongodb_write_fn)

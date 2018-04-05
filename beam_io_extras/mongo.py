from __future__ import absolute_import
from apache_beam.transforms import DoFn
from apache_beam.transforms import ParDo
from apache_beam.transforms import PTransform


class MongoDBWriteFn(DoFn):
    """

    """

    def __init__(self,
                 database_name=None,
                 collection_name=None,
                 db_client=None,
                 batch_size=None,
                 test_client=None):

        self.database_name = database_name
        self.collection_name = collection_name
        self.db_client = db_client

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
        self.db_client[self.database_name][self.collection_name].insert_many(self._rows_buffer)
        self._rows_buffer = []


class WriteToMongoDB(PTransform):

    def __init__(self,
                 database_name=None,
                 collection_name=None,
                 db_client=None,
                 batch_size=None,
                 test_client=None):
        """
        Initialize a WriteToMongoDB transform.

        """
        self.database_name = database_name
        self.collection_name = collection_name
        self.db_client = db_client

        self.batch_size = batch_size
        self.test_client = test_client

    def expand(self, pcoll):
        mongodb_write_fn = MongoDBWriteFn(
            database_name=self.database_name,
            collection_name=self.collection_name,
            db_client=self.db_client,
            test_client=self.test_client)
        return pcoll | 'WriteToMongoDB' >> ParDo(mongodb_write_fn)

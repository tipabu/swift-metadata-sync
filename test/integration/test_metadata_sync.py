import elasticsearch
import json
import logging
import os
import random
import string
import swiftclient
import unittest
import utils


class MetadataSyncTest(unittest.TestCase):
    ES_HOST = 'https://localhost:9200'
    ES_VERSION = os.environ['ES_VERSION']

    def _get_container(self, container=None):
        if not container:
            container = u'\062a' + ''.join([
                random.choice(string.ascii_lowercase) for _ in range(8)])
        self.client.put_container(container)
        self.containers.append(container)
        return container

    def _get_index(self, index=None):
        if not index:
            index = ''.join([
                random.choice(string.ascii_lowercase) for _ in range(8)])
        if self.es_conn.indices.exists(index):
            self.es_conn.indices.delete(index)
        self.es_conn.indices.create(index, include_type_name=False)
        self.indices.append(index)
        return index

    def setUp(self):
        self.logger = logging.getLogger('test-metadata-sync')
        self.logger.addHandler(logging.StreamHandler())
        self.client = swiftclient.client.Connection(
            'http://localhost:8080/auth/v1.0',
            u'\u062aacct:\u062auser',
            u'\u062apass')
        self.es_conn = utils.get_es_connection(
            self.ES_HOST, True, utils.get_ca_cert(self.ES_VERSION))
        self.containers = []
        self.indices = []
        self.index = self._get_index()
        self.container = self._get_container()
        self.config = {
            'containers': [
                {'account': u'AUTH_\u062aacct',
                 'container': self.container,
                 'index': self.index,
                 'es_hosts': self.ES_HOST,
                 'verify_certs': True,
                 'ca_certs': utils.get_ca_cert(self.ES_VERSION)}
            ],
        }
        self.indexer = utils.get_metadata_sync_instance(
            self.config, self.logger)

    def tearDown(self):
        for container in self.containers:
            _, listing = self.client.get_container(container)
            for entry in listing:
                self.client.delete_object(container, entry['name'])
            self.client.delete_container(container)
        self.containers = []

        for index in self.indices:
            self.es_conn.indices.delete(index)

    def test_index_regular_objects(self):
        object_name = u'\u062a-object'
        self.client.put_object(
            self.container, object_name, 'stuff',
            headers={'x-object-meta-foo': 'sample meta',
                     u'x-object-meta-\u062a-bar': u'unicode h\u00e9ader'})

        self.indexer.run_once()

        doc_id = utils.get_doc_id(self.config['containers'][0]['account'],
                                  self.container, object_name)
        es_doc = self.es_conn.get(self.index, doc_id)
        self.assertEqual('sample meta', es_doc['_source']['foo'])
        self.assertEqual(u'unicode h\u00e9ader',
                         es_doc['_source'][u'\u062a-bar'])

    def test_removes_documents(self):
        object_name = u'\u062a-object'
        self.client.put_object(
            self.container, object_name, 'stuff',
            headers={'x-object-meta-foo': 'sample meta',
                     u'x-object-meta-\u062a-bar': u'unicode h\u00e9ader'})

        self.indexer.run_once()

        # Elasticsearch client will raise an exception if the document ID is
        # not found
        doc_id = utils.get_doc_id(self.config['containers'][0]['account'],
                                  self.container, object_name)
        self.es_conn.get(self.index, doc_id)

        self.client.delete_object(self.container, object_name)
        self.indexer.run_once()

        with self.assertRaises(elasticsearch.TransportError) as ctx:
            self.es_conn.get(self.index, doc_id)
        self.assertEqual(404, ctx.exception.status_code)

    def test_indexes_slos(self):
        segments_container = self._get_container()
        manifest = []
        for i in range(2):
            self.client.put_object(segments_container, 'part-%d' % i,
                                   chr((ord('A') + i)) * 1024)
            manifest.append(
                {'path': '/'.join((segments_container, 'part-%d' % i))})
        slo_key = u'SLO-\u062a'
        self.client.put_object(
            self.container, slo_key, json.dumps(manifest),
            query_string='multipart-manifest=put',
            headers={u'x-object-meta-sl\u00f6': u'valu\ue009'})

        self.indexer.run_once()

        doc_id = utils.get_doc_id(self.config['containers'][0]['account'],
                                  self.container, slo_key)
        resp = self.es_conn.get(self.index, doc_id)
        self.assertEqual('true', resp['_source']['x-static-large-object'])
        self.assertEqual(u'valu\ue009', resp['_source'][u'sl\u00f6'])


class MetadataSync6xTest(MetadataSyncTest):
    ES_HOST = 'https://localhost:9201'
    ES_VERSION = os.environ['OLD_ES_VERSION']

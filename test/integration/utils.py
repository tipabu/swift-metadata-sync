import collections
from container_crawler import crawler
import elasticsearch
import hashlib
import json
from swift_metadata_sync import metadata_sync


class EphemeralMetadataSync(metadata_sync.MetadataSync):
    '''Keeps the MetadataSync status in memory to be used for testing.'''
    def __init__(self, status_dir, settings, per_account=False, status={}):
        super(EphemeralMetadataSync, self).__init__(
            status_dir, settings, per_account)
        self._status = status

    def _get_row(self, row_field, db_id):
        # NOTE: we ignore the specific index constraint
        if db_id not in self._status:
            return 0
        if row_field not in self._status[db_id]:
            return 0
        return self._status[db_id][row_field]

    def _save_row(self, row_id, row_field, db_id):
        # NOTE: we ignore the specific index constraint
        if db_id not in self._status:
            self._status[db_id] = {}
        self._status[db_id][row_field] = row_id


class EphemeralMetadataSyncFactory(metadata_sync.MetadataSyncFactory):
    def __init__(self, config):
        super(EphemeralMetadataSyncFactory, self).__init__(config)
        self._statuses = collections.defaultdict(dict)

    def instance(self, settings, **kwargs):
        status = self._statuses[json.dumps(settings, sort_keys=True)]
        return EphemeralMetadataSync(
            self._conf['status_dir'], settings, status=status)


def get_metadata_sync_instance(conf, logger):
    sync_conf = {
        'devices': '/swift/nodes/1/node',
        'items_chunk': 1000,
        'log_file': '/var/log/swift-metadata-sync.log',
        'poll_interval': 1,
        'status_dir': '/tmp/',
        'workers': 10,
        'bulk_process': True
    }
    sync_conf.update(conf)
    return crawler.Crawler(
        sync_conf, EphemeralMetadataSyncFactory(sync_conf), logger)


def get_es_connection(host, verify_certs, ca_certs):
    return elasticsearch.Elasticsearch(
        host, verify_certs=verify_certs, ca_certs=ca_certs)


def get_ca_cert(version):
    return '/'.join((
        '', 'elasticsearch-{}'.format(version),
        'config', 'ca', 'ca.crt'))


def get_doc_id(account, container, key):
    return hashlib.sha256(
        '/'.join(map(lambda part: part.encode('utf-8'),
                     (account, container, key)))).hexdigest()

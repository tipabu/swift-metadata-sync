import email.utils
import json
import logging
import os
import os.path

import kafka

from swift.common.request_helpers import strip_user_meta_prefix, \
    get_user_meta_prefix
from swift.common.utils import decode_timestamps
from swift.common.memcached import MemcacheRing
from container_crawler.base_sync import BaseSync
from kafka.errors import KafkaTimeoutError, NoBrokersAvailable


class KafkaSync(BaseSync):
    DOC_TYPE = 'object'
    DOC_MAPPING = {
        "content-length": {"type": "long"},
        "content-type": {"type": "string"},
        "etag": {"type": "string"},
        "last-modified": {"type": "date"},
        "x-object-manifest": {"type": "string"},
        "x-static-large-object": {"type": "boolean"},
        "x-swift-container": {"type": "string"},
        "x-swift-account": {"type": "string"},
        "x-swift-object": {"type": "string"},
        "x-timestamp": {"type": "date"},
        "x-trans-id": {"type": "string"}
    }
    USER_META_PREFIX = get_user_meta_prefix('object')

    PROCESSED_ROW = 'last_row'
    VERIFIED_ROW = 'last_verified_row'

    def __init__(self, status_dir, settings, per_account=False):
        super(KafkaSync, self).__init__(status_dir, settings, per_account)

        self.logger = logging.getLogger('swift-kafka-sync')
        kafka_servers = settings['kafka_servers']
        # TODO kwargs (ca_cert, etc...)
        try:
            self._producer = kafka.KafkaProducer(
                bootstrap_servers=kafka_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        except NoBrokersAvailable:
            raise RuntimeError('No Kafka Brokers available.'
                               ' Check kafka_servers option')
        self._topic = settings['topic']
        self._parse_json = settings.get('parse_json', False)
        self._cluster_id = settings.get('cluster_id')
        if not self._cluster_id:
            raise RuntimeError('Configuration option "cluster_id" is missing')

        # verify memcache value time to expire
        self._verify_cache_expiration = settings.get('verify_cache_life', 60)

        memcache_servers = settings.get('memcache_servers')
        if not memcache_servers:
            raise RuntimeError('Configuration option "memcache_servers" is'
                               ' missing')

        # TODO: let's go with defaults for now
        self._memcache = MemcacheRing(
            [s.strip().encode('utf-8')
             for s in memcache_servers.split(',') if s.strip()])

    def _get_row(self, row_field, db_id):
        if not os.path.exists(self._status_file):
            return 0
        with open(self._status_file) as f:
            try:
                status = json.load(f)
                entry = status.get(db_id, None)
                if not entry:
                    return 0
                if entry['topic'] == self._topic:
                    try:
                        return entry[row_field]
                    except KeyError:
                        if row_field == self.VERIFIED_ROW:
                            return entry.get(self.PROCESSED_ROW, 0)
                return 0
            except ValueError:
                return 0

    def _save_row(self, row_id, row_field, db_id):
        if not os.path.exists(self._status_account_dir):
            os.mkdir(self._status_account_dir)
        if not os.path.exists(self._status_file):
            new_rows = {self.PROCESSED_ROW: 0,
                        self.VERIFIED_ROW: 0,
                        'topic': self._topic}
            new_rows[row_field] = row_id
            with open(self._status_file, 'w') as f:
                json.dump({db_id: new_rows}, f)
                return

        with open(self._status_file, 'r+') as f:
            try:
                status = json.load(f)
            except ValueError:
                status = {}
            new_rows = {'topic': self._topic,
                        self.PROCESSED_ROW: 0,
                        self.VERIFIED_ROW: 0}
            if db_id in status:
                old_processed_row = status[db_id][self.PROCESSED_ROW]
                new_rows[self.PROCESSED_ROW] = old_processed_row
                new_rows[self.VERIFIED_ROW] =\
                    status[db_id].get(self.VERIFIED_ROW, old_processed_row)
            new_rows[row_field] = row_id

            status[db_id] = new_rows
            f.seek(0)
            json.dump(status, f)
            f.truncate()

    def get_last_processed_row(self, db_id):
        return self._get_row(self.PROCESSED_ROW, db_id)

    def get_last_verified_row(self, db_id):
        return self._get_row(self.VERIFIED_ROW, db_id)

    def save_last_processed_row(self, row_id, db_id):
        return self._save_row(row_id, self.PROCESSED_ROW, db_id)

    def save_last_verified_row(self, row_id, db_id):
        return self._save_row(row_id, self.VERIFIED_ROW, db_id)

    def handle(self, rows, internal_client):
        self.logger.debug("Handling rows: %s" % repr(rows))
        if not rows:
            return

        for row in rows:
            msg_key = self._get_message_key(row)

            # check if a message has already been sent for this row
            # if so, skip it
            cached_ts = self._memcache.get(msg_key)
            if cached_ts:
                _, _, cached_meta = decode_timestamps(cached_ts)
                _, _, row_meta = decode_timestamps(row['created_at'])
                if cached_meta >= row_meta:
                    continue

            if row['deleted']:
                msg = self._create_delete_message(row)
            else:
                msg = self._create_update_message(row, internal_client)
            try:
                self._producer.send(self._topic,
                                    key=msg_key,
                                    value=msg)
                # TODO: is msg_key=ts enough to signal this row
                # has been processed?
                self._memcache.set(msg_key, row['created_at'],
                                   time=self._verify_cache_expiration)
            except KafkaTimeoutError:
                raise RuntimeError('Failed to send message about %s/%s/%s' % (
                    self._account, self._container,
                    row['name'].decode('utf-8')))

    def _create_delete_message(self, row):
        msg = {}
        msg['op-type'] = 'delete'
        msg['x-swift-object'] = row['name'].decode('utf-8')
        msg['x-swift-account'] = self._account
        msg['x-swift-container'] = self._container
        timestamp = decode_timestamps(row['created_at'])
        ts = email.utils.mktime_tz(
            email.utils.parsedate_tz(timestamp)) * 1000
        msg['last-modified'] = ts
        return msg

    def _create_update_message(self, row, internal_client, parse_json=False):
        def _parse_meta_value(value):
            try:
                return json.loads(value.decode('utf-8'))
            except ValueError:
                return value.decode('utf-8')

        swift_hdrs = {'X-Newest': True}
        meta = internal_client.get_object_metadata(
            self._account, self._container, row['name'], headers=swift_hdrs)
        msg = {}

        # use the format of the timstamp to differentiate between PUT and POST
        ts, content_ts, meta_ts = decode_timestamps(row['created_at'],
                                                    explicit=True)

        # TODO: should we compare row ts and HEAD response ts,
        # what if they are different?
        if content_ts or meta_ts:
            msg['op-type'] = 'post'
        else:
            msg['op-type'] = 'put'

        # TODO: code is very similar to metadata_sync create_es_doc
        # maybe move to a base class
        msg['x-timestamp'] = meta['x-timestamp']
        # Convert Last-Modified header into a millis since epoch date
        ts = email.utils.mktime_tz(
            email.utils.parsedate_tz(meta['last-modified'])) * 1000
        msg['last-modified'] = ts
        msg['x-swift-object'] = row['name'].decode('utf-8')
        msg['x-swift-account'] = self._account
        msg['x-swift-container'] = self._container

        user_meta_keys = dict(
            [(strip_user_meta_prefix('object', k).decode('utf-8'),
              _parse_meta_value(v) if parse_json else v.decode('utf-8'))
             for k, v in meta.items()
             if k.startswith(get_user_meta_prefix('object'))])
        msg.update(user_meta_keys)
        for field in KafkaSync.DOC_MAPPING.keys():
            if field in msg:
                continue
            if field not in meta:
                continue
            if KafkaSync.DOC_MAPPING[field]['type'] == 'boolean':
                msg[field] = str(meta[field]).lower()
                continue
            msg[field] = meta[field]
        return msg

    def _get_message_key(self, row):
        return '/'.join([self._cluster_id.encode('utf-8'),
                         self._account.encode('utf-8'),
                         self._container.encode('utf-8'),
                         row['name']])

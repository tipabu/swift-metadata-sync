from metadata_sync import MetadataSync
from kafka_sync import KafkaSync


class MetadataSyncFactory(object):
    def __init__(self, config):
        self._conf = config
        if not config.get('status_dir'):
            raise RuntimeError('Configuration option "status_dir" is missing')

    def __str__(self):
        return 'MetadataSyncFactory'

    def instance(self, settings, per_account=False):
        provider = settings.get('meta_sync_provider', 'elastic_search')
        if provider == 'elastic_search':
            return MetadataSync(
                self._conf['status_dir'], settings, per_account=per_account)
        else:
            return KafkaSync(
                self._conf['status_dir'], settings, per_account=per_account)

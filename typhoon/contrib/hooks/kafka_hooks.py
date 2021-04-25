from typhoon.connections import ConnectionParams
from typhoon.contrib.hooks.hook_interface import HookInterface


class KafkaConsumerHook(HookInterface):
    conn_type = 'kafka_consumer'

    def __init__(self, conn_params: ConnectionParams):
        self.conn_params = conn_params

    def __enter__(self) -> 'KafkaConsumer':
        from kafka import KafkaConsumer
        self.conn = KafkaConsumer(
            bootstrap_servers=self.conn_params.login or self.conn_params.extra.get('bootstrap_servers'),
            client_id=self.conn_params.extra.get('client_id') or 'typhoon',
            group_id=self.conn_params.extra.get('group_id'),
        )
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close(self.conn_params.extra.get('autocommit', True))
        self.conn = None


class KafkaProducerHook(HookInterface):
    conn_type = 'kafka_producer'

    def __init__(self, conn_params: ConnectionParams):
        self.conn_params = conn_params

    def __enter__(self) -> 'KafkaProducer':
        from kafka import KafkaProducer
        self.conn = KafkaProducer(
            bootstrap_servers=self.conn_params.login or self.conn_params.extra.get('bootstrap_servers'),
            client_id=self.conn_params.extra.get('client_id') or 'typhoon',
            compression_type=self.conn_params.extra.get('compression_type'),
            batch_size=self.conn_params.extra.get('batch_size'),
        )
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()
        self.conn = None

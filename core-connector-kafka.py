import time
import uuid
import struct

from confluent_kafka.cimpl import Producer

from proto.com.mts.bigdata.api.core.bd_auth2_pb2 import CoreConnector, CoreConnectorKey, CoreConnetorType

d_time = int(time.time()) * 1000


def gen_value():
    a = uuid.UUID('00000000-0000-0000-0000-000000000000').bytes
    high, low = struct.unpack(">QQ", a)
    print(high, low)
    # uce_value_pb2.UUID(low=low, high=high)

    data = {
        'id': {'low': low, 'high': high},
        'level': 'SECURITY_LEVEL_CONSUMER',
        'clientId': 9,
        'active' : True,
        'type': 'CONNECTOR_TYPE_DC',
        'dc': {'appId': {'low': low, 'high': high},
               'key': 'qeqweqweqweqweqwdqdqd',
               'secret': 'qdwdqdqdqcqcqcqcqcqq'
               },
        'name': 'connadmin'}
    value = CoreConnector(**data)

    d_k = {'id': {'low': low, 'high': high}}

    key = CoreConnectorKey(**d_k)
    print(value)
    return key.SerializeToString(), value.SerializeToString()


def gen():
    producer_config = {
        "bootstrap.servers": "localhost:29092"
    }

    producer = Producer(producer_config)

    producer.poll(0)

    item = gen_value()
    producer.produce(topic="topic3", key=item[0],
                     value=item[1],
                     # timestamp=int(time.time()) - 10000
                     )
    producer.flush()


gen()

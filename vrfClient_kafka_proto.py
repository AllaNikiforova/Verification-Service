import struct
import time
import uuid

from confluent_kafka.cimpl import Producer

from proto.com.mts.bigdata.api.verification import bd_vrf_backend_pb2

a = uuid.UUID('9f853a18-18bd-4f2d-8a7c-75990fcfa64c').bytes
low, high = struct.unpack(">QQ", a)

d_time = int(time.time()) * 1000


def gen_value(clientId):
    data = {'clientId': clientId,
            'maxBatchSize': 10,
            'objects': [
                {
                    'type': 'VRF_OBJECT_TYPE_ID_TRAFFIC',
                    'async': False,
                    'paymentId': {'low': 9978980174601889356, 'high': 11494657499101744941},
                    'paymentGroup': 1,
                    'enabled': False
                },
                {
                    'type': 'VRF_OBJECT_TYPE_PD_DOCS',
                    'async': False,
                    'paymentId': {'low': 9978980174601889356, 'high': 11494657499101744941},
                    'paymentGroup': 1,
                    'enabled': True
                },
                {
                    'type': 'VRF_OBJECT_TYPE_PD_IDENTITY',
                    'async': False,
                    'paymentId': {'low': 9978980174601889356, 'high': 11494657499101744941},
                    'paymentGroup': 1,
                    'enabled': True
                },
                {
                    'type': 'VRF_OBJECT_TYPE_PD_BIRTHDATE',
                    'async': False,
                    'paymentId': {'low': 9978980174601889356, 'high': 11494657499101744941},
                    'paymentGroup': 1,
                    'enabled': True
                },
                {
                    'type': 'VRF_OBJECT_TYPE_ID_CARD',
                    'async': False,
                    'paymentId': {'low': 9978980174601889356, 'high': 11494657499101744941},
                    'paymentGroup': 1,
                    'enabled': True
                }
            ],
            'active': False,
            'op': 'OP_CREATE',
            'createdAt': 1724329142532,
            'createdBy': {'low': 9978980174601889356, 'high': 11494657499101744941}, 'updatedAt': 1724329142532,
            'updatedBy': {'low': 9978980174601889356, 'high': 11494657499101744941},
            'additionalProperties' : {'hostPattern' : 'test_host_pattern'}
            }
    # data = {}
    value = bd_vrf_backend_pb2.VrfClient(**data)

    d_k = {'clientId': clientId}

    key = bd_vrf_backend_pb2.VrfClientKey(**d_k)
    # key_instance = bd_vrf_backend_pb2.VrfClientKey()
    # key_instance.clientId = clientId

    # value_instance = bd_vrf_backend_pb2.VrfClient()
    # value_instance.clientId = clientId
    # value_instance.maxBatchSize = 10
    # value_instance.active = True
    # value_instance.op = 'OP_CREATE'
    # value_instance.createdAt = 1724329142532
    print(value)

    return key.SerializeToString(), value.SerializeToString()


producer_config = {
        "bootstrap.servers": "localhost:29092"
}

producer = Producer(producer_config)

def gen(num):
    producer.poll(0)
    for _ in range(1):
        for item in [gen_value(9 + i) for i in range(num)]:
            producer.produce(topic="uce13", key=item[0],
                             value=item[1],
                             )
        producer.flush()


NUM_MES = 1
gen(NUM_MES)

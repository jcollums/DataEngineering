#!/home/jcollums/Group-Project/dataeng/bin/python3
from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
import os

if __name__ == '__main__':
    path = f'.'
    files = [f for f in os.listdir(path) if os.path.isfile(f'{path}/{f}') and f.endswith('.json')]
    for filename in files:
        args = ccloud_lib.parse_args()
        config_file = args.config_file
        topic = 'archivetest2'
        conf = ccloud_lib.read_ccloud_config(config_file)

        producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
        producer = Producer(producer_conf)

        ccloud_lib.create_topic(conf, topic)
        delivered_records = 0

        def acked(err, msg):
            global delivered_records
            """Delivery report handler called on
            successful or failed delivery of message
            """
            if err is not None:
                print("Failed to deliver message: {}".format(err))
            else:
                delivered_records += 1

        breadcrumbs = open(f'{path}/{filename}')
        breadcrumbs = json.load(breadcrumbs)

        for datum in breadcrumbs:
            record_value = json.dumps(datum, indent=2).encode('utf-8')
            producer.produce(topic, value=record_value, on_delivery=acked)
            producer.poll(0)

        # os.rename(f'{path}/{filename}', f'{path}/processed/{filename}')
        
        print(f"Delivered {delivered_records} records")
        producer.flush()

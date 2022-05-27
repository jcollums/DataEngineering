from confluent_kafka import Consumer
from datetime import date
from time import time
import json
import ccloud_lib

if __name__ == '__main__':
    start_time = time()

    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = "archivetest2"
    conf = ccloud_lib.read_ccloud_config(config_file)

    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'main.group'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)
    
    consumer.subscribe([topic])

    total_count = 0
    message_list = []
    msg_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None:
                print(msg.value())
            else:
                break

    except KeyboardInterrupt:
        pass
    finally:
        print("Closing consumer")
        consumer.close()

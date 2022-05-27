#!/home/jcollums/Group-Project/dataeng/bin/python3
from confluent_kafka import Consumer
from datetime import date
from time import time
import json
import ccloud_lib
import zlib
from google.cloud import storage

if __name__ == '__main__':
    start_time = time()

    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = "archivetest2"
    conf = ccloud_lib.read_ccloud_config(config_file)

    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'archive.group'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)

    consumer.subscribe([topic])
    path = '.' 

    total_count = 0
    message_list = []
    msg_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if len(message_list) > 0:
                    print("Writing file")

                    original_data = json.dumps(message_list).encode('utf-8')
                    compressed_data = zlib.compress(original_data, zlib.Z_BEST_COMPRESSION)
                    filename = f'{path}/compressed/{date.today()}.zlib'
                    f = open(filename, 'wb')
                    f.write(compressed_data)
                    f.close()

                    compress_ratio = (float(len(original_data)) - float(len(compressed_data))) / float(len(original_data))
                    print('Compressed: %d%%' % (100.0 * compress_ratio))
                    print('Uploading to bucket')

                    # Instantiates a client
                    client = storage.Client()

                    # Creates a new bucket and uploads an object
                    new_bucket = client.get_bucket('jcollums_dataeng_week8')
                    new_blob = new_bucket.blob(filename)
                    new_blob.upload_from_filename(filename=filename)
                    break
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                record_value = msg.value()
                data = json.loads(record_value)

                msg_count += 1
                if msg_count % 100 == 0:
                    print('Consumed 100 messages')

                message_list.append(data)

    except KeyboardInterrupt:
        pass
    finally:
        print("Closing consumer")
        consumer.close()

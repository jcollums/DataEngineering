#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
from time import sleep
from datetime import datetime
from random import randint


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    producer_conf['transactional.id'] = datetime.now().time()
    producer = Producer(producer_conf)

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))
    
    breadcrumbs = open('bcsample.json')
    breadcrumbs = json.load(breadcrumbs)
    crumb_id = 0

    client_key = randint(0,100)
    counter = 0
    counter2 = 0

    producer.init_transactions()
    producer.begin_transaction()
    for crumb in breadcrumbs:
        crumb_id += 1
        record_key = str(randint(1,5))
        crumb['count'] = crumb_id
        
        record_value = json.dumps(crumb).encode('utf-8')
        
        print(f"Client {client_key} Producing key {record_key}")
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked) 

        counter = counter + 1
        counter2 = counter2 + 1
        if (counter % 4 == 0):
            sleep(2)
            if randint(0,1) == 1:
                producer.commit_transaction()
                print("Committed")
            else:
                producer.abort_transaction()
                print("Aborted")
            producer.begin_transaction()

    producer.flush()
        

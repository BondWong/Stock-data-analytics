# - read from kafka
# - write to redis queue

from kafka import KafkaConsumer

import argparse
import atexit
import logging
import redis

topic_name = ""
kafka_broker = ""

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('redis-publisher')
logger.setLevel(logging.DEBUG)

def shutdown_hook(kafka_consumer):
	logging.info('shutdonw kafka consumer')
	kafka_consumer.close(10)

if __name__ == '__main__':
	# - setup arguments
	parser = argparse.ArgumentParser()
	parser.add_argument('topic_name', help='the kafka topic to read from')
	parser.add_argument('kafka_broker', help='location of kafka broker')
	parser.add_argument('redis_channel', help='the redis channel to publish to')
	parser.add_argument('redis_host', help='redis server ip')
	parser.add_argument('redis_port', help='redis server port')

	args = parser.parse_args()
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker
	redis_channel = args.redis_channel
	redis_host = args.redis_host
	redis_port = args.redis_port

	# - setup kafka consumer
	kafka_consumer = KafkaConsumer(topic_name, bootstrap_servers=kafka_broker)

	# - setup redis client
	redis_client = redis.StrictRedis(host=redis_host, port=redis_port)

	# - write msg to redis message queue
	for msg in kafka_consumer:
		logger.info('receive data %s' % str(msg))
		redis_client.publish(redis_channel, msg.value)

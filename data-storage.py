# - read from kafka
# - write to cassandra

from kafka import KafkaConsumer
from kafka.errors import KafkaError, KafkaTimeoutError
from cassandra.cluster import Cluster

import argparse
import logging
import json
import atexit

# - logging
logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)

# - default value

topic_name = 'stock-analyzer'
kafka_broker = '127.0.0.1:9092'
keyspace = 'stock'
data_table = ''
cassandra_broker = '127.0.0.1:9042'

def persist_data(stock_data, cassandra_session):
	"""
	@param stock_data dict
	@param cassandra_session, a session created using cassnadra-driver
	"""
	stock_data = stock_data.decode('utf-8')
	logger.debug('store data %s into cassandra', stock_data)
	parsed = json.loads(stock_data)[0]
	symbol = parsed.get('StockSymbol')
	price = float(parsed.get('LastTradePrice'))
	time = parsed.get('LastTradeTime')

	statement = "INSERT INTO %s (stock_symbol, trade_price, trade_time) VALUES ('%s', %f, '%s')" % (data_table, symbol, price, time)
	cassandra_session.execute(statement)
	logger.info('Persist data into cassandra for symbol: %s, price %f, tradetime %s', symbol, price, time)

def shutdown_hoot(consumer, session):
	consumer.close()
	session.shutdown()

if __name__ == '__main__':
	# - setup commandline arguments
	parser = argparse.ArgumentParser()
	parser.add_argument('topic_name', help='the kafka topic')
	parser.add_argument('kafka_broker', help='the location of kafka broker')
	parser.add_argument('keyspace', help='the keyspace to be used in cassandra')
	parser.add_argument('data_table', help='data table to be used')
	parser.add_argument('cassandra_broker', help='ip and port of cassandra node')

	# - parse argument
	args = parser.parse_args()
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker
	keyspace = args.keyspace
	data_table = args.data_table
	cassandra_broker = args.cassandra_broker

	# - setup kafka consumer
	consumer = KafkaConsumer(topic_name, bootstrap_servers=kafka_broker)
	
	# - setup cassandra session
	cassandra_cluster = Cluster(contact_points=cassandra_broker.split(','))
	session = cassandra_cluster.connect(keyspace)

	atexit.register(shutdown_hoot, consumer, session)

	for msg in consumer:
		persist_data(msg.value, session)

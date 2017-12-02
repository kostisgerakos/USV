from kafka import KafkaConsumer
import avro.schema
import avro.io
import io
import json
import sqlite3
import requests
import io, random
from avro.io import DatumWriter
import struct
import sys
import math

#Load Configuration
config = {}
try:
	with open('Config/config.json', 'r') as MPconfig:
		config = json.load(MPconfig)
except:
	print "Unexpected error:", sys.exc_info()[0]
	raise
	sys.exit()

print json.dumps(config, indent=2)

#Get schema from schema registry based on schemaID
def getSchema(schema_id):
	try:
		reg_url = config['schemaRegistryURL'] + '/schemas/ids/' + str(schema_id)
		response = requests.get(reg_url)
		if response.status_code == 200:
			registry_schema = json.loads(response.text)['schema']
			return avro.schema.parse(registry_schema), json.loads(registry_schema)['name']
		return None
	except:
		print "Unexpected error:", sys.exc_info()[0]
		raise
		return None

class ConsumerDB(object):
	"""docstring for ConsumerDB"""
	def __init__(self, db='C:\Program Files (x86)\Mission Planner\Scripts\MP.db'):
		super(ConsumerDB, self).__init__()
		self.db = db
		self.conn = None
		
	def _open(self):
		try:
			self.conn = sqlite3.connect(self.db)
			#print "open database " + self.db
		except Exception as e:
			print str(e)

	def _close(self):
		try:
			self.conn.close()
			#print "close database " + self.db
		except Exception as e:
			print str(e)

	def execute(self, sql="SELECT last_insert_rowid()"):
		self._open()
		c = self.conn.cursor()
		c.execute(sql)
		c.execute("SELECT last_insert_rowid()")
		last_id = c.fetchone()
		self.conn.commit()
		self._close()
		return last_id[0]

#Load Kafka Consumer
consumer = None
consumerDB = None
mission_counter = 0
waypoint_counter = 0

try:
	consumer = KafkaConsumer(config['goToTopic'], group_id=config['goToGroup'], bootstrap_servers=[config['kafkaURL']])
	consumerDB = ConsumerDB(config['databasePath'])
except:
	print "Unexpected error:", sys.exc_info()[0]
	raise
	sys.exit()

try:
	#Geting messages
	for msg in consumer:
		if msg.partition != config['statisticsPartition']:
			continue
		print "Received message.."
		magic_byte = struct.unpack('b', msg.value[0])
		schema_id = struct.unpack('>I', msg.value[1:5])[0]
		schema, name = getSchema(schema_id)
		msg2 = msg.value[5:]
		bytes_reader = io.BytesIO(msg2)
		decoder = avro.io.BinaryDecoder(bytes_reader)
		reader = avro.io.DatumReader(schema)
		payload = reader.read(decoder)
		print "Message decoded"

		print json.dumps(payload, indent=2)
		if name == "Goto":
			mission_name = "MISSION_" + str(mission_counter)
			sql = "INSERT INTO MPMissions (name, status, type) VALUES ('" + str(mission_name) + "', 'PENDING', 'WAYPOINTS');"
			mission_id = consumerDB.execute(sql)
			print mission_name + " ADDED to Database [ID]: "+str(mission_id) 

			mission_counter = mission_counter + 1
			waypoint_counter = waypoint_counter + 1

			lat = str(payload['location']['latitude']*180/math.pi)
			lng = str(payload['location']['longitude']*180/math.pi)
			print lat, lng
			sql = "INSERT INTO MPCommands (missionID, command, latitude, longitude, sequence, speed, radius) VALUES (" + str(mission_id) + ", 'WAYPOINT', " + lat + ", '" + lng + "', '" + str(waypoint_counter) + "', -1, -1);"
			consumerDB.execute(sql)
			print "GoTo: "+lat+", "+lng+" ADDED To Database [MissionID]: "+ str(mission_id)

			sql = "UPDATE MPMissions set status='READY' WHERE id=" + str(mission_id) + ";"
			consumerDB.execute(sql)
			print "Mission "+str(mission_id)+" is READY to execute"
		else:
			print "Expected GoTo Command..."
except:
	print "Unexpected error:", sys.exc_info()[0]
	raise
	sys.exit()

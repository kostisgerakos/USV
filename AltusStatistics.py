import sqlite3
import sys
import time
import math
import avro.schema
import json
import requests
from kafka import SimpleProducer, KafkaClient, KafkaProducer
import io, random
from avro.io import DatumWriter
import struct

headers = {"Content-Type" : "application/vnd.schemaregistry.v1+json"}
#URL = "http://10.124.10.207:8081/subjects/"
#URL = "http://172.19.0.18:8081/subjects/"

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

def LoadAvsc(file_path, names=None):
    """
    Load avsc file
    file_path: path to schema file
    names(optional): avro.schema.Names object
    """
    file_text = open(file_path).read()
    json_data = json.loads(file_text)
    schema = avro.schema.make_avsc_object(json_data, names)
    return schema

class Statistics(object):
	"""docstring for Statistics"""
	def __init__(self, db, sourceSystem="", sourceModule=""):
		super(Statistics, self).__init__()
		self.db = db
		self.conn = None
		self.sourceSystem = sourceSystem
		self.sourceModule = sourceModule

		self.groundSpeed = None
		self.lat = None
		self.lng = None
		self.gpsstatus = None
		self.satcount = None
		self.roll = None
		self.pitch = None
		self.yaw = None
		self.battery_voltage = None
		self.battery_remaining = None
		self.ax = None
		self.ay = None
		self.az = None
		self.mx = None
		self.my = None
		self.mz = None
		self.alt = None

		self.location =        {"data" : None, "schema" : None, "schemaID" : -1}
		self.attitude =        {"data" : None, "schema" : None, "schemaID" : -1}
		self.voltage =         {"data" : None, "schema" : None, "schemaID" : -1}
		self.fuelUsage =       {"data" : None, "schema" : None, "schemaID" : -1}
		self.linearVelocity =  {"data" : None, "schema" : None, "schemaID" : -1}
		self.angularVelocity = {"data" : None, "schema" : None, "schemaID" : -1}

		self.ready = self.read()

	def _getSchema(self, schemas=[]):
		known_schemas = avro.schema.Names()
		param_schema = None
		for i in range((len(schemas))):
			if i == (len(schemas) - 1):
				param_schema = LoadAvsc(schemas[i], known_schemas)
			else:
				sub_schemas = LoadAvsc(schemas[i], known_schemas)
		
		schema = param_schema.to_json(avro.schema.Names())
		subject = "USV_" + schema['name']
		reg_schema = {"schema" : json.dumps(schema)}
		schema = avro.schema.parse(json.dumps(schema))

		

		response = requests.post(config['schemaRegistryURL'] + "/subjects/" + subject + '/versions', data=json.dumps(reg_schema), headers=headers)
		print config['schemaRegistryURL'] + "/subjects/" + subject + '/versions',
		schema_id = -1
		print response.text
		if response.status_code == 200:
			schema_id = int(json.loads(response.text)['id'])
		return schema, schema_id

	def _getHeader(self, unix_time=None):
		if unix_time == None:
			unix_time = time.time()
		return {
      			"sourceSystem": self.sourceSystem,
      			"sourceModule": self.sourceModule,
      			"time": long(unix_time)
	   	}

	def _getAttitude(self, roll=0.0, pitch=0.0, yaw=0.0, header=None):
		if header == None:
			header = self._getHeader()
		return  {
					"header": header,
					"phi": roll*math.pi/180,
					"theta": pitch*math.pi/180,
					"psi": yaw*math.pi/180
				}

	def _getVelocity(self, x=0.0, y=0.0, z=0.0, header=None):
		if header == None:
			header = self._getHeader()
		return  {
			   		"header": header,
			   		"x": x,
		   			"y": y,
		   			"z": z
				} 

	def _getLocation(self, lat=0.0, lng=0.0, alt=0.0, header=None):
		if header == None:
			header = self._getHeader()
		return {
			   		"header": header,
			   		"latitude": lat*math.pi/180,
		   			"longitude": lng*math.pi/180,
		   			"height": alt,
		   			"n" : 0.0,
		   			"e" : 0.0,
		   			"d" : 0.0,
		   			"depth" : None,
		   			"altitude" : None
				}

	def _getVoltage(self, value=0.0, header=None):
		if header == None:
			header = self._getHeader()
		return {
			   		"header": header,
			   		"value": float(value)
				} 

	def _getFuelUsage(self, value=0, header=None):
		if header == None:
			header = self._getHeader()
		return {
			   		"header": header,
			   		"value": int(value)
				}

	def open(self):
		try:
			self.conn = sqlite3.connect(self.db)
		except Exception as e:
			print str(e)

	def close(self):
		try:
			self.conn.close()
		except Exception as e:
			print str(e)

	def read(self):
		try:
			self.open()
			sql = "SELECT * FROM MPStatus ORDER By id DESC LIMIT 1";
			c = self.conn.cursor()
			c.execute(sql)
			self.conn.commit()
			data = c.fetchall()[0]
			self.close()

			self.groundSpeed = data[1]
			self.lat = data[2]
			self.lng = data[3]
			self.gpsstatus = data[4]
			self.satcount = data[5]
			self.roll = data[6]
			self.pitch = data[7]
			self.yaw = data[8]
			self.battery_voltage = data[9]
			self.battery_remaining = data[10]
			self.ax = data[11]
			self.ay = data[12]
			self.az = data[13]
			self.mx = data[14]
			self.my = data[15]
			self.mz = data[16]
			self.alt = data[17]

			self.location['data']        = self._getLocation(lat=self.lat, lng=self.lng, alt=self.alt)
			self.location['schema'], self.location['schemaID'] =  self._getSchema([config['statisticsPath']['header'], config['statisticsPath']['location']])

			self.attitude['data']        = self._getAttitude(roll=self.roll, pitch=self.pitch, yaw=self.yaw)
			self.attitude['schema'], self.attitude['schemaID'] =  self._getSchema([config['statisticsPath']['header'], config['statisticsPath']['attitude']])

			self.voltage['data']         = self._getVoltage(value=self.battery_voltage)
			self.voltage['schema'], self.voltage['schemaID'] =  self._getSchema([config['statisticsPath']['header'], config['statisticsPath']['voltage']])

			self.fuelUsage['data']      = self._getFuelUsage(value=self.battery_voltage)
			self.fuelUsage['schema'], self.fuelUsage['schemaID'] =  self._getSchema([config['statisticsPath']['header'], config['statisticsPath']['fuel_usage']])

			self.linearVelocity['data']  = self._getVelocity(x=self.ax, y=self.ay, z=self.az)
			self.linearVelocity['schema'], self.linearVelocity['schemaID'] =  self._getSchema([config['statisticsPath']['header'], config['statisticsPath']['linear_velocity']])

			self.angularVelocity['data'] = self._getVelocity(x=self.mx, y=self.my, z=self.mz)
			self.angularVelocity['schema'], self.angularVelocity['schemaID'] =  self._getSchema([config['statisticsPath']['header'], config['statisticsPath']['angular_velocity']])
			return True
		except:
			print "No data to send..."
			self.close()
			return False

#db='C:\Program Files (x86)\Mission Planner\Scripts\MP.db'
#statistics = Statistics(db,"System", "Module")
print config['databasePath']
statistics = Statistics(config['databasePath'], config['statisticsSystem'], config['statisticsModule'])
try:
	#kafka = KafkaClient(config['kafkaURL'])
	#producer = SimpleProducer(kafka)
	producer = KafkaProducer(bootstrap_servers=config['kafkaURL'])

	#topic = "altus-status"
	#print producer
	while True:
		if statistics.ready == False:
			time.sleep(config['statisticsDelay'])
			statistics.ready = statistics.read()
			continue
		#Send Location
		writer = avro.io.DatumWriter(statistics.location['schema'])
		bytes_writer = io.BytesIO()
		encoder = avro.io.BinaryEncoder(bytes_writer)
		writer.write(statistics.location['data'], encoder)
		raw_bytes = bytes_writer.getvalue()
		magic_byte = struct.pack('b', 0)
		schema_id_byte = struct.pack('>I',statistics.location['schemaID'])
		raw_bytes = magic_byte + schema_id_byte + raw_bytes
		topic = config['statisticsTopic']['location']
		producer.send(topic=topic, value=raw_bytes, partition=config['statisticsPartition'])

		#Send attitude
		writer = avro.io.DatumWriter(statistics.attitude['schema'])
		bytes_writer = io.BytesIO()
		encoder = avro.io.BinaryEncoder(bytes_writer)
		writer.write(statistics.attitude['data'], encoder)
		raw_bytes = bytes_writer.getvalue()
		magic_byte = struct.pack('b', 0)
		schema_id_byte = struct.pack('>I',statistics.attitude['schemaID'])
		raw_bytes = magic_byte + schema_id_byte + raw_bytes
		topic = config['statisticsTopic']['attitude']
		producer.send(topic=topic, value=raw_bytes, partition=config['statisticsPartition'])

		#Send voltage
		writer = avro.io.DatumWriter(statistics.voltage['schema'])
		bytes_writer = io.BytesIO()
		encoder = avro.io.BinaryEncoder(bytes_writer)
		writer.write(statistics.voltage['data'], encoder)
		raw_bytes = bytes_writer.getvalue()
		magic_byte = struct.pack('b', 0)
		schema_id_byte = struct.pack('>I',statistics.voltage['schemaID'])
		raw_bytes = magic_byte + schema_id_byte + raw_bytes

		topic = config['statisticsTopic']['voltage']
		producer.send(topic=topic, value=raw_bytes, partition=config['statisticsPartition'])

		#Send fuelUsage
		writer = avro.io.DatumWriter(statistics.fuelUsage['schema'])
		bytes_writer = io.BytesIO()
		encoder = avro.io.BinaryEncoder(bytes_writer)
		writer.write(statistics.fuelUsage['data'], encoder)
		raw_bytes = bytes_writer.getvalue()
		magic_byte = struct.pack('b', 0)
		schema_id_byte = struct.pack('>I',statistics.fuelUsage['schemaID'])
		raw_bytes = magic_byte + schema_id_byte + raw_bytes
		topic = config['statisticsTopic']['fuel_usage']
		producer.send(topic=topic, value=raw_bytes, partition=config['statisticsPartition'])

		#Send linearVelocity
		writer = avro.io.DatumWriter(statistics.linearVelocity['schema'])
		bytes_writer = io.BytesIO()
		encoder = avro.io.BinaryEncoder(bytes_writer)
		writer.write(statistics.linearVelocity['data'], encoder)
		raw_bytes = bytes_writer.getvalue()
		magic_byte = struct.pack('b', 0)
		schema_id_byte = struct.pack('>I',statistics.linearVelocity['schemaID'])
		raw_bytes = magic_byte + schema_id_byte + raw_bytes
		topic = config['statisticsTopic']['linear_velocity']
		producer.send(topic=topic, value=raw_bytes, partition=config['statisticsPartition'])

		#Send angularVelocity
		writer = avro.io.DatumWriter(statistics.angularVelocity['schema'])
		bytes_writer = io.BytesIO()
		encoder = avro.io.BinaryEncoder(bytes_writer)
		writer.write(statistics.angularVelocity['data'], encoder)
		raw_bytes = bytes_writer.getvalue()
		magic_byte = struct.pack('b', 0)
		schema_id_byte = struct.pack('>I',statistics.angularVelocity['schemaID'])
		raw_bytes = magic_byte + schema_id_byte + raw_bytes
		topic = config['statisticsTopic']['angular_velocity']
		producer.send(topic=topic, value=raw_bytes, partition=config['statisticsPartition'])

		time.sleep(config['statisticsDelay'])
		statistics.read()
except Exception as e:
	print str(e)
	print "closing db"
finally:
	#conn.close()
	statistics.close()
import sys
import math
import clr
import time
import System
from System import Byte

clr.AddReference("MissionPlanner")
import MissionPlanner
clr.AddReference("MissionPlanner.Utilities") # includes the Utilities class
from MissionPlanner.Utilities import Locationwp
clr.AddReference("MAVLink") # includes the Utilities class
import MAVLink
#sys.path.append(r"C:\Python27\Lib\site-packages")
#sys.path.append(r"C:\Python27\Lib")
sys.path.append(r"C:\Program Files\IronPython 2.7\Lib")
import json

import threading

#Load Configuration
configPath = 'C:\\ALTUS_USV\\Config\\config.json' #FULL PATH
config = {}
try:
	with open(configPath, 'r') as MPconfig:
		config = json.load(MPconfig)
except:
	print "Unexpected error:", sys.exc_info()[0]
	raise
	sys.exit()

DB_MISSIONS = "SELECT id FROM MPMissions WHERE status="
DB_MISSION_ABORT = "SELECT status FROM MPMissions WHERE id="
DB_COMMANDS = "SELECT * FROM MPCommands WHERE missionID="
DB_MISSION_UPDATE = "UPDATE MPMissions SET status='COMPLETED' WHERE id="
DB_HOME = "SELECT HomeLat, HomeLng from MPConfig WHERE id = 1;"
DB_ALL_ABORT = "SELECT Aborted FROM MPConfig WHERE id=1;"

INTERVAL = config['MP_INTERVAL'] #1000 #ms
STAT_INTERVAL = config['MP_STAT_INTERVAL'] #1.0
t = None

import sqlite3
import System
import time

print 'Start Script'

import MissionPlanner

class Command(object):
	"""docstring for Command"""
	def __init__(self, id, missionID, command, sequence, latitude, longitude, speed=-1, radius=-1):
		super(Command, self).__init__()
		self.id = id
		self.missionID = missionID
		self.command = command
		self.sequence = sequence
		self.latitude = latitude
		self.longitude = longitude
		self.speed = speed
		self.radius = radius
		self.completed = False
		

class MissionPlannerDB(object):
	"""docstring for MissionPlannerDB"""

	def __init__(self, db='C:\Program Files (x86)\Mission Planner\Scripts\MP.db'):
		super(MissionPlannerDB, self).__init__()
		self.db = db
		self.conn = None

	def open(self):
		try:
			self.conn = sqlite3.connect(self.db, timeout=10)
			#print "open database " + self.db
		except Exception as e:
			print str(e)

	def close(self):
		try:
			self.conn.commit()
			self.conn.close()
			#print "close database " + self.db
		except Exception as e:
			print str(e)

	def updateStatistics(self):#, groundSpeed=0.0, lat=0.0, lng=0.0):
		#global t
		try:
			self.open()
			c = self.conn.cursor()
			date_time = System.DateTime.Now
			timestamp = date_time.ToString("yyyy-MM-ddTHH:mm:ss.fff")
			values = "'"+str(cs.groundspeed)+"','"+str(cs.lat)+"','"+str(cs.lng)+"','"+str(cs.gpsstatus)+"','"+str(cs.satcount)+"','"+str(cs.roll)+"','"+str(cs.pitch)+"','"+str(cs.yaw)+"','"+str(cs.battery_voltage)+"','"+str(cs.battery_remaining)+"','"+str(cs.ax)+"','"+str(cs.ay)+"','"+str(cs.az)+"','"+str(cs.mx)+"','"+str(cs.my)+"','"+str(cs.mz)+"','"+str(timestamp)+"'"
			sql = "INSERT INTO MPStatus (groundSpeed, lat, lng, gpsstatus, satcount, roll, pitch, yaw, battery_voltage, battery_remaining, ax, ay, az, mx, my, mz, timestamp) VALUES ("+values+");"
			timeout = 10
			for x in range(0, timeout):
				try:
					c.execute(sql)
					self.conn.commit()
					self.close()
					print "Inserting data..."
					break
				except:
					print "faild to insert data, try again.."
					Script.Sleep(100)
			#t = threading.Timer(STAT_INTERVAL, self.updateStatistics)
			#t.start()
		except Exception as e:
			print "exception: ",str(e)
			self.close()

	def completeMission(self, missionID):
		sql = DB_MISSION_UPDATE + str(missionID) + ";"
		c = self.conn.cursor()
		c.execute(sql)
		self.conn.commit()
		#missionIDs = c.fetchall()
		#return [missionID[0] for missionID in missionIDs]

	def getMissions(self, status="READY"):
		sql = DB_MISSIONS + "'" + status + "';"
		c = self.conn.cursor()
		c.execute(sql)
		self.conn.commit()
		missionIDs = c.fetchall()
		return [missionID[0] for missionID in missionIDs]

	def getCommands(self, missionID=-1):
		if missionID == -1:
			return []
		sql = DB_COMMANDS + str(missionID) + ";"
		c = self.conn.cursor()
		c.execute(sql)
		self.conn.commit()
		commands = c.fetchall()
		data = []
		for cmd in commands:
			command = Command(id=cmd[0], missionID=cmd[1], command=cmd[2], sequence=cmd[3], latitude=cmd[4], longitude=cmd[5], speed=cmd[6], radius=cmd[7])
			data.append(command)
		return data

	def getHome(self):
		sql = DB_HOME
		c = self.conn.cursor()
		c.execute(sql)
		self.conn.commit()
		location = c.fetchall()[0]
		return location[0], location[1]

	def isAborted(self, missionID=None):
		if missionID == None:
			return True
		sql = DB_MISSION_ABORT + str(missionID) + ";"
		c = self.conn.cursor()
		c.execute(sql)
		self.conn.commit()
		status = c.fetchall()[0][0]
		print "status ", status
		if status == "ABORTED":
			return True
		return False

	def isAllAborted(self):
		sql = DB_ALL_ABORT
		c = self.conn.cursor()
		c.execute(sql)
		self.conn.commit()
		status = c.fetchall()[0][0]
		print "status ", status
		if status == 1:
			return True
		return False


class Missions(object):
	"""docstring for Missions"""
	def __init__(self, pendingMissions=[]):
		super(Missions, self).__init__()
		self.pendingMissions = list(set(pendingMissions))

	def addMissions(self, missionIDs=[]):
		self.pendingMissions = missionIDs #self.pendingMissions + missionIDs
		self.pendingMissions = list(set(self.pendingMissions))

	def addMission(self, missionID=None):
		if missionID == None:
			return
		if missionID not in self.pendingMissions:
			self.pendingMissions.append(missionID)

	def getNextMission(self):
		if len(self.pendingMissions) == 0:
			return -1
		return self.pendingMissions[0]

	def completeMission(self, missionID=None):
		if missionID == None:
			return
		if missionID in self.pendingMissions:
			self.pendingMissions.remove(missionID)

class Mission(object):
	"""docstring for Mission"""
	def __init__(self, commands=[]):
		super(Mission, self).__init__()
		self.commands = commands

	def _getUncompetedSize(self):
		return len([cmd for cmd in self.commands if cmd.completed==True])

	def _getNextCommand(self):
		nextCommand = None
		for cmd in self.commands:
			if nextCommand == None:
				nextCommand = cmd
				continue
			if nextCommand.sequence > cmd.sequence:
				nextCommand = cmd
				continue
		return nextCommand

	def prepare(self, checkForAbort=True, lat=35.490163, lng=24.064234):
		idmavcmd = MAVLink.MAV_CMD.WAYPOINT
		id = int(idmavcmd)
		#lat = 35.490163
		#lng = 24.064234
		home = Locationwp().Set(lat, lng,0, id)
		print "upload home - reset on arm"
		
		waypoints = []
		#for i in range(0, len(self.commands)):# self._getUncompetedSize()):
		#	command = self._getNextCommand()
		i = 1
		for command in self.commands:
			if command.command == "WAYPOINT":
				waypoint = Locationwp().Set(float(command.latitude), float(command.longitude),0, id)
				waypoints.append((i, waypoint))
				i = i + 1
		print "set wp total ",len(waypoints)
		MAV.setWPTotal(len(waypoints)+1)
		MAV.setWP(home,0,MAVLink.MAV_FRAME.GLOBAL_RELATIVE_ALT);
		for waypoint in waypoints:
			print "seting waypoint ",waypoint[1], waypoint[0]
			MAV.setWP(waypoint[1], waypoint[0], MAVLink.MAV_FRAME.GLOBAL_RELATIVE_ALT);
		MAV.setWPACK();

	def execute(self, missionID=None, checkForAbort=True):
		#STAT_INTERVAL = 1.0
		if missionID == None:
			return False
		aborted = True
		print "Ready to execute Mission"
		#Script.ChangeMode("Guided")
		Script.ChangeMode("Auto")
		print "setting mode to Auto"
		while cs.mode != "Auto":
			print "whating for Auto status..."
			Script.Sleep(100)
		MAV.doARM(True);
		print "do Arm"
		onMission = False
		missionPlannerDB = MissionPlannerDB(config['databasePath'])
		while True:
			Script.Sleep(INTERVAL)
			missionPlannerDB.updateStatistics()
			print cs.mode
			if cs.mode == "HOLD":# and onMission == True:
				print "break"
				break
			# if cs.mode == "Auto":
			# 	print "Mission Start..."
			# 	onMission = True
			missionPlannerDB.open()
			isAborted = missionPlannerDB.isAborted(missionID)
			missionPlannerDB.close()
			if isAborted:
				print isAborted
				aborted = False
				print "aborted"
				Script.ChangeMode("HOLD")
				break
				#Script.ChangeMode("Manual")
				#MAV.doARM(False);
				#missionPlannerDB.close()
				#return False
			
		#for i in range(0,5):
		#	data = str(i) + ": " + str(cs.groundspeed) +" , "+ str(cs.lat) +" , "+ str(cs.lng)
		# 	print("enter: " + data)
		# 	Script.Sleep(1000)
		#Script.ChangeMode("Manual")
		#while cs.mode != "Manual":
		#	print "whating for Manual status..."
		#	Script.Sleep(100)
		#MAV.doARM(False);
		return aborted

#st = time.time()
try:
	#Load database
	missionPlannerDB = MissionPlannerDB(config['databasePath'])
	#Load missions handler
	missions = Missions()
	while True:
		#Check and execute every X Sec
		Script.Sleep(INTERVAL)
		#Insert updated statistics to DB
		missionPlannerDB.updateStatistics()

		#Get next mission
		missionPlannerDB.open()
		missions.addMissions(missionPlannerDB.getMissions())
		missionPlannerDB.close()
		
		missionID = missions.getNextMission()
		if missionID == -1:
			print "No missions to be execute..."
			STAT_INTERVAL = 10.0
			continue
		missionPlannerDB.open()
		mission = Mission(missionPlannerDB.getCommands(missionID))
		missionPlannerDB.close()

		missionPlannerDB.open()
		lat, lng = missionPlannerDB.getHome()
		missionPlannerDB.close()

		#print commandIDs
		mission.prepare(lat=lat, lng=lng)
		completed = mission.execute(missionID=missionID)
		completed = True
		if completed:
			missionPlannerDB.open()
			missionPlannerDB.completeMission(missionID)
			missionPlannerDB.close()
			missions.completeMission(missionID)

except Exception as e:
	print "exit program... " + str(e)
finally:
	missionPlannerDB.close()
	print "closing db..."
	Script.ChangeMode("Manual")
	MAV.doARM(False);
	#t.cancel()

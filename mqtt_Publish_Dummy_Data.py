#------------------------------------------
#--- Author: Pradeep Singh
#--- Date: 20th January 2017
#--- Version: 1.0
#--- Python Ver: 2.7
#--- Details At: https://iotbytes.wordpress.com/store-mqtt-data-from-sensors-into-sql-database/
#------------------------------------------


import paho.mqtt.client as mqtt
import random, threading, json
from datetime import datetime
from near_location import *
import sys

#====================================================
# MQTT Settings 
MQTT_Broker = 'broker.emqx.io'
MQTT_Port = 1883
Keep_Alive_Interval = 45
MQTT_Topic = "cloud2020/JustSeba/sensor_data"


#====================================================

def on_connect(client, userdata, rc):
	if rc != 0:
		pass
		print("Unable to connect to MQTT Broker...")
	else:
		print("Connected with MQTT Broker: " + str(MQTT_Broker))

def on_publish(client, userdata, mid):
	pass
		
def on_disconnect(client, userdata, rc):
	if rc !=0:
		pass
		
mqttc = mqtt.Client()
mqttc.on_connect = on_connect
mqttc.on_disconnect = on_disconnect
mqttc.on_publish = on_publish
mqttc.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))		

		
def publish_To_Topic(topic, message):
	mqttc.publish(topic,message)
	print ("Published: " + str(message) + " " + "on MQTT Topic: " + str(topic))



#====================================================
# FAKE SENSOR 
# Dummy code used as Fake Sensor to publish some random values
# to MQTT Broker

toggle = 0

class SensorData:
	def __init__(self,id):
		self.temperature=round(random.uniform(-20,60), 2)
		self.humidity=random.randint(10,90)
		self.pollution=abs(random.randint(-25,75))
		self.id=id
		self.latitude=near_location(50.09286500280051, 19.92465692590898, 50)
		self.date=datetime.now()

	def createJSON(self):
		sensorData = {}
		sensorData['Sensor_ID'] = self.id
		sensorData['Date'] = self.date.strftime("%d-%b-%Y %H:%M:%S:%f")
		sensorData['Humidity'] = self.humidity
		json_data = json.dumps(sensorData)

		print(f'Publishing sensor data: {self.id} \n '
			  f'Temperature: {self.temperature}\n '
			  f'Humidity: {self.humidity}\n '
			  f'Latitude: {self.latitude}\n\n')
		return json_data


sensorID = 0


def publish_Fake_Sensor_Values_to_MQTT():
	threading.Timer(3.0, publish_Fake_Sensor_Values_to_MQTT).start()
	global sensorID
	sd = SensorData(sensorID)
	publish_To_Topic(MQTT_Topic, sd.createJSON())



def main():
	if sys.argv[1].isdigit():
		global sensorID
		sensorID = sys.argv[1]

		publish_Fake_Sensor_Values_to_MQTT()
	else:
		print("Add sensor id")

if __name__ == "__main__":
    main()




#====================================================

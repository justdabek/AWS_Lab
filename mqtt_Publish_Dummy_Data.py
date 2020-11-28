# ------------------------------------------
# --- Author: Pradeep Singh
# --- Date: 20th January 2017
# --- Version: 1.0
# --- Python Ver: 2.7
# --- Details At: https://iotbytes.wordpress.com/store-mqtt-data-from-sensors-into-sql-database/
# ------------------------------------------


import paho.mqtt.client as mqtt
import random, threading, json
from datetime import datetime
from near_location import *
import sys
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time


def on_connect(client, userdata, rc):
    if rc != 0:
        pass
        print("Unable to connect to MQTT Broker...")
    else:
        print("Connected")
        pass
    # print("Connected with MQTT Broker: " + str(MQTT_Broker))


def on_publish(client, userdata, mid):
    pass


def on_disconnect(client, userdata, rc):
    if rc != 0:
        pass


# Custom MQTT message callback
def customCallback(client, userdata, message):
    print("Received a new message: ")
    print(message.payload)
    print("from topic: ")
    print(message.topic)
    print("--------------\n\n")

# ====================================================
# FAKE SENSOR 
# Dummy code used as Fake Sensor to publish some random values
# to MQTT Broker

class Sensor:
    def __init__(self, sensor_id):
        self.sensor_id = sensor_id
        self.temperature = None
        self.humidity = None
        self.pollution = None
        self.latitude = None
        self.date = None
        self.measure_data()
        # # MQTT Settings
        # self.mqttc = None
        # self.MQTT_Broker = 'broker.emqx.io'
        # self.MQTT_Port = 1883
        # self.Keep_Alive_Interval = 45
        # self.MQTT_Topic = "cloud2020/JustSeba/sensor_" + str(sensor_id)

        # MQTT Settings
        self.mqttc = None
        self.MQTT_Topic = "cloud2020/JustSeba/sensor_" + str(sensor_id)
        self.AWSClientName = "AWSPython"
        self.AWSPort = 8883
        self.endpoint = "arn:aws:iot:us-east-1:445268892728:thing/AWSPython"
        self.basePathToCerts = r"C:\Users\AMD\Desktop\Python\Studia\Chmury\AWS_Lab\AWSCertificates"
        self.rootCAPath = self.basePathToCerts + r"\rootca.pem"
        self.privateKeyPath = self.basePathToCerts + r"\dafe660620-private.pem.key"
        self.certificatePath = self.basePathToCerts + r"\dafe660620-certificate.pem.crt"

    def measure_data(self):
        self.temperature = round(random.uniform(-20, 60), 2)
        self.humidity = random.randint(10, 90)
        self.pollution = abs(random.randint(-25, 75))
        self.latitude = near_location(50.09286500280051, 19.92465692590898, 50)
        self.date = datetime.now().strftime("%d-%b-%Y %H:%M:%S:%f")

    def createJSON(self):
        sensor_data = {'Sensor_ID': self.sensor_id, 'Date': self.date, 'Humidity': self.humidity,
                       'Temperature': self.temperature, 'Pollution': self.pollution, 'Latitude': self.latitude}
        json_data = json.dumps(sensor_data)

        print(f'Publishing sensor data: {self.sensor_id} \n '
              f'Temperature: {self.temperature}\n '
              f'Humidity: {self.humidity}\n '
              f'Latitude: {self.latitude}\n\n')
        return json_data

    def publish_Fake_Sensor_Values_to_MQTT(self):
        threading.Timer(3.0, self.publish_Fake_Sensor_Values_to_MQTT).start()
        self.measure_data()
        self.publish_To_Topic(self.MQTT_Topic, self.createJSON())

    # def connect_sensor(self):
    #     self.mqttc = mqtt.Client()
    #     self.mqttc.on_connect = on_connect
    #     self.mqttc.on_disconnect = on_disconnect
    #     self.mqttc.on_publish = on_publish
    #     self.mqttc.connect(self.MQTT_Broker, int(self.MQTT_Port), int(self.Keep_Alive_Interval))

    def connect_sensor(self):
        self.mqttc = AWSIoTMQTTClient(self.AWSClientName)
        self.mqttc.configureEndpoint(self.endpoint, self.AWSPort)
        self.mqttc.configureCredentials(self.rootCAPath, self.privateKeyPath, self.certificatePath)

        # AWSIoTMQTTClient connection configuration
        self.mqttc.configureAutoReconnectBackoffTime(1, 32, 20)
        self.mqttc.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
        self.mqttc.configureDrainingFrequency(2)  # Draining: 2 Hz
        self.mqttc.configureConnectDisconnectTimeout(10)  # 10 sec
        self.mqttc.configureMQTTOperationTimeout(5)  # 5 sec

        self.mqttc.connect()

        self.mqttc.subscribe(self.MQTT_Topic, 1, customCallback)

        time.sleep(2)

        # self.mqttc = mqtt.Client()
        # self.mqttc.on_connect = on_connect
        # self.mqttc.on_disconnect = on_disconnect
        # self.mqttc.on_publish = on_publish
        # self.mqttc.connect(self.MQTT_Broker, int(self.MQTT_Port), int(self.Keep_Alive_Interval))

    def publish_To_Topic(self, topic, message):
        self.mqttc.publish(topic, message)
        print("Published: " + str(message) + " " + "on MQTT Topic: " + str(topic))


def main():
    if sys.argv[1].isdigit():
        sensorID = sys.argv[1]
        sensor = Sensor(sensorID)
        sensor.connect_sensor()
        sensor.publish_Fake_Sensor_Values_to_MQTT()
    else:
        print("Add sensor id")


if __name__ == "__main__":
    main()

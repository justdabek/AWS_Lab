# ------------------------------------------
# --- Author: Pradeep Singh
# --- Date: 20th January 2017
# --- Version: 1.0
# --- Python Ver: 2.7
# --- Details At: https://iotbytes.wordpress.com/store-mqtt-data-from-sensors-into-sql-database/
# ------------------------------------------

import paho.mqtt.client as mqtt
from store_Sensor_Data_to_DB import sensor_Data_Handler
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time


class SensorListener:
    def __init__(self):
        self.data_list = []
        # MQTT Settings
        self.mqttc = None
        self.MQTT_Topic_Get = "cloud2020/JustSeba/#"
        self.MQTT_Topic_Group = "cloud2020/JustSeba/DataGroup"
        self.AWSClientName = "SensorListener"
        self.AWSPort = 8883
        self.endpoint = "a13fbctsul0zeq-ats.iot.us-east-1.amazonaws.com"
        self.basePathToCerts = r"C:\Users\AMD\Desktop\Python\Studia\Chmury\AWS_Lab\AWSCertificates\just"
        self.rootCAPath = self.basePathToCerts + r"\root-CA.crt"
        self.privateKeyPath = self.basePathToCerts + r"\SensorPolicy.private.key"
        self.certificatePath = self.basePathToCerts + r"\SensorPolicy.cert.pem"


    def connect_to_broker(self):
        self.mqttc = AWSIoTMQTTClient(self.AWSClientName)
        self.mqttc.configureEndpoint(self.endpoint, self.AWSPort)
        self.mqttc.configureCredentials(self.rootCAPath, self.privateKeyPath, self.certificatePath)

        # AWSIoTMQTTClient connection configuration
        self.mqttc.configureAutoReconnectBackoffTime(1, 32, 20)
        self.mqttc.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
        self.mqttc.configureDrainingFrequency(2)  # Draining: 2 Hz
        self.mqttc.configureConnectDisconnectTimeout(10)  # 10 sec
        self.mqttc.configureMQTTOperationTimeout(20)  # 20 sec

        self.mqttc.connect()
        print('Connected')

        self.mqttc.subscribe(self.MQTT_Topic_Get, 1, self.customCallback)
        self.mqttc.subscribe(self.MQTT_Topic_Group, 1, self.groupCallback)

    def publish_To_Topic(self, topic, message):
        self.mqttc.publish(topic, message, 1)
        print("Published: " + str(message) + " " + "on MQTT Topic: " + str(topic))

    # Custom MQTT message callback
    def customCallback(self, client, userdata, message):
        print("ping")
        print(message.payload)
        self.data_list.append(message.payload.decode("utf-8"))
        # print("Received a new message: ")
        # print(message.payload)
        # print("from topic: ")
        # print(message.topic)
        # print("--------------\n\n")

    def sender(self):
        while True:
            time.sleep(0.001)
            # print(len(self.data_list))
            if len(self.data_list) >= 10:
                for payload in self.data_list:
                    self.publish_To_Topic(self.MQTT_Topic_Group, str(payload))
                print("Published")
                self.data_list = []

    def groupCallback(self, client, userdata, message):
        print("Message sent")

def main():
    sL = SensorListener()
    sL.connect_to_broker()
    sL.sender()
    # while True:
    #     time.sleep(1)



if __name__ == "__main__":
    main()


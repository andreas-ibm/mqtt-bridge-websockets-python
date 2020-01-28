# MQTT Standalone bridge for sending data using WebSockets
import argparse
import paho.mqtt.client as paho
import time
import threading
import uuid
from flask import Flask



parser = argparse.ArgumentParser()
parser.add_argument("-s","--sourcebroker", help="The hostname of the broker to subscribe to")
parser.add_argument("-p","--sourceport", type=int, help="The port of the broker to subscribe to", default=1883)
parser.add_argument("-d","--targetbroker", help="The hostname of the broker to publish to")
parser.add_argument("-o","--targetport", type=int, help="The port of the broker to publish to", default=9001)
parser.add_argument("-e","--endpoint",help="The endpoint to register the edge broker as, defaults to ws://<sourcebroker>:9001")
parser.add_argument("-t","--topic", help="The topic to bridge", default='#')
parser.add_argument("-v","--verbose", help="Be verbose about relaying messages", action="store_true")

args = parser.parse_args()

app = Flask(__name__)
app.debug = False
@app.route('/')
def hello():
   return "Bridge config: Bridging {} from {}({}) to {}({})".format(arguments.topic, arguments.sourcebroker, arguments.sourceport, arguments.targetbroker, arguments.targetport)

def on_subscribe(client, userdata, mid, granted_qos):   #create function for callback
   print("subscribed with qos",granted_qos, "\n")
   pass

def on_target_message(client, userdata, message):
   ## we didn't really expect to receive anything here...
   print("message received from target  "  ,str(message.payload.decode("utf-8")))
   
def on_publish(client,userdata,mid):   #create function for callback
   if args.verbose:
      print("data published mid=",mid, "\n")
   pass

def on_disconnect(client, userdata, rc):
   print("client disconnected ok")

def on_source_connect(client, userdata, flags, rc):
   print("Connected to source broker with rc={}".format(rc))
   print("  subscribing to ",args.topic)
   client.subscribe(args.topic)

   

def main(arguments):
   threading.Thread(target=app.run).start()
   print("Bridge config: Bridging {} from {}({}) to {}({})".format(arguments.topic, arguments.sourcebroker, arguments.sourceport, arguments.targetbroker, arguments.targetport))
   # the function that will do the actual bridging
   def on_source_message(client, userdata, message):
      ## this needs to pass on to the target
      if arguments.verbose:
         print("message received from source:\n\t{}\n\t{}".format(message.topic,str(message.payload.decode("utf-8"))))
      publisher.publish(message.topic, message.payload)

   # connect to the target broker
   id = str(uuid.uuid4().fields[-1])[:5]
   subscriber = paho.Client("mqtt-bridge-source-"+id)
   # we want to pass a lot of messages around
   subscriber.max_inflight_messages_set(300)
   # Use callback functions, pass them in
   subscriber.on_subscribe = on_subscribe  
   subscriber.on_publish = on_publish      
   subscriber.on_message = on_source_message      
   subscriber.on_disconnect = on_disconnect
   subscriber.on_connect = on_source_connect
   print("connecting to broker ",arguments.sourcebroker,"on port ",arguments.sourceport)
   subscriber.connect(arguments.sourcebroker,arguments.sourceport)

   # connect to the target broker
   will = ""
   topic_will = "mqtt/edge"
   publisher = paho.Client("mqtt-bridge-target-"+id,transport='websockets')
   publisher.will_set(topic_will, payload=will, qos=0, retain=False)
   publisher.max_inflight_messages_set(300)
   # use callback functions, some are the same as the source broker.
   publisher.on_subscribe = on_subscribe   
   publisher.on_publish = on_publish       
   publisher.on_message = on_target_message
   publisher.on_disconnect = on_disconnect
   print("connecting to broker ",arguments.targetbroker,"on port ",arguments.targetport)
   publisher.connect(arguments.targetbroker,arguments.targetport)
   # Tell the broker that there is now an edge broker it's getting data from
   endpoint = "ws://"+arguments.sourcebroker+":9001"
   if arguments.endpoint is not None:
      endpoint = arguments.endpoint
   print("publishing edge endpoint "+endpoint+" to broker")
   
   publisher.publish(topic_will, endpoint, retain=True)
   publisher.loop_start()

   # keep going foverever!
   subscriber.loop_forever()


   
if __name__ == "__main__":
   main(args)
      

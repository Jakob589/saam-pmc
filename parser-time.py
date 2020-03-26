#!/usr/bin/python3

import json
import paho.mqtt.client as mqtt
import time
import os

client = mqtt.Client()
client.username_pw_set("", "") ## add password
client.tls_set(ca_certs="/opt/cert/ca_certificate.pem",
                certfile="/opt/cert/client_certificate.pem",
                keyfile="/opt/cert/client_key.pem")
client.tls_insecure_set(True)
client.connect("mqtt.saam-platform.eu", 8883)

start_time = int(round(time.time()*1000))

loc_id = open('loc-id').readline().strip()

mqtt_topic = "saam/data/" + loc_id + "/sens_power_"
mqtt_topic_event = "saam/data/"+ loc_id +"/sens_power_event_"

energy_json = {
            "timestamp": 12345566,
            "timestep": 3600000,
            "measurements": [ 234.5 ]
            }

event_json = {
            "timestamp": 12345566,
            "timestep": -1,
            "measurements": [ {"dP": 23.5, "dQ":-12.6} ]
}


def calculated_time(x):
    
    strX = str(x) # to string

    named_tuple = time.localtime() # get struct_time on #pmc you should use local time
    time_string = time.strftime("%m/%d/%Y,%H:%M:%S", named_tuple) #tuple to string

    #solving edge problem where if current time is less that the one that is being parsed 
    #In that case, the parsed data is from the day before, or in case midnight problem
    if x > int(time_string[11:13]):
        time_string = time_string[:3] + str(int(time_string[3:5])-1) + time_string[5:11] + strX + ":00:00" + time_string[19:] 
    else:
        time_string = time_string[:11] + strX + ":00:00" + time_string[19:] 
    
    calculated_time = int(time.mktime(time.strptime(time_string, "%m/%d/%Y,%H:%M:%S"))*1000)
    
    return calculated_time


for x in range(24):
    file_name = "out_by_app_f1_" + str(x) + ".csv"
    
    
    try:
        
        with open(file_name, "r") as ins:
            for line in ins:
            
                if "Energy" in line:
                    energy_json["timestamp"] = calculated_time(x)
                    energy_json["measurements"] = [float(line.strip().split(':')[1])]

                    t = mqtt_topic + "f1_" + "energy"
                    print(t)
                    print(json.dumps(energy_json))
                    client.publish(topic=t, payload=json.dumps(energy_json))

                else:
                    l = line.strip().split(':')
                    event = l[0]
                    d = l[1].split(',')

                    event_json["timestamp"] = int(d[0])
                    event_json["measurements"] = [ {"dP": float(d[1]), "dQ": float(d[2])} ] 

                    t = mqtt_topic_event + "f1_" + line.strip().split(':')[0]
                    print(t)
                    print(json.dumps(event_json))
                    client.publish(topic=t, payload=json.dumps(event_json)) 
        os.remove(file_name)
        file_name2 = "out_by_app_f2_" + str(x) + ".csv"

        with open(file_name2, "r") as ins:
            for line in ins:
                
                if "Energy" in line:
                    energy_json["timestamp"] = calculated_time(x)
                    energy_json["measurements"] = [float(line.strip().split(':')[1])]

                    t = mqtt_topic + "f2_" + "energy"
                    print(t)
                    print(json.dumps(energy_json))
                    client.publish(topic=t, payload=json.dumps(energy_json))

                else:
                    l = line.strip().split(':')
                    event = l[0]
                    d = l[1].split(',')

                    event_json["timestamp"] = int(d[0])
                    event_json["measurements"] = [ {"dP": float(d[1]), "dQ": float(d[2])} ] 

                    t = mqtt_topic_event + "f2_" + line.strip().split(':')[0]
                    print(t)
                    print(json.dumps(event_json))
                    client.publish(topic=t, payload=json.dumps(event_json))
        os.remove(file_name2)
    

        file_name3 = "out_by_app_f3_" + str(x) + ".csv"

        with open(file_name3, "r") as ins:
            for line in ins:
            
                if "Energy" in line:
                    energy_json["timestamp"] = calculated_time(x)
                    energy_json["measurements"] = [float(line.strip().split(':')[1])]

                    t = mqtt_topic + "f3_" + "energy"
                    print(t)
                    print(json.dumps(energy_json))
                    client.publish(topic=t, payload=json.dumps(energy_json))

                else:
                    l = line.strip().split(':')
                    event = l[0]
                    d = l[1].split(',')

                    event_json["timestamp"] = int(d[0])
                    event_json["measurements"] = [ {"dP": float(d[1]), "dQ": float(d[2])} ] 

                    t = mqtt_topic_event + "f3_" + line.strip().split(':')[0]
                    print(t)
                    print(json.dumps(event_json))
                    client.publish(topic=t, payload=json.dumps(event_json))
            os.remove(file_name3)
    
    except IOError:
        pass



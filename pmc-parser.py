import json
import paho.mqtt.client as mqtt

client = mqtt.Client()
client.connect("localhost")

start_time = 1559001600

mqtt_topic = "saam/data/mihas/sens_power_"
mqtt_topic_event = "saam/data/mihas/sens_power_event_"

energy_json = {
            "timestamp": 12345566,
            "timestep": 3600,
            "measurements": [ 234.5 ]
            }

event_json = {
            "timestamp": 12345566,
            "timestep": -1,
            "measurements": [ {"dP": 23.5, "dQ":-12.6} ]
}

for x in range(24):
    file_name = "out_by_app_f1_" + str(x) + ".csv"

    with open(file_name, "r") as ins:
        for line in ins:
            if "dump" not in line:
                if "Energy" in line:
                    energy_json["timestamp"] = start_time
                    energy_json["measurements"] = [float(line.strip().split(':')[1])]

                    t = mqtt_topic + "f1_" + "energy"
                    print(t)
                    print(json.dumps(energy_json))
                    client.publish(topic=t, payload=json.dumps(energy_json))

                else:
                    l = line.strip().split(':')
                    event = l[0]
                    d = l[1].split(',')

                    event_json["timestamp"] = int(int(d[0])/1000)
                    event_json["measurements"] = [ {"dP": float(d[1]), "dQ": float(d[2])} ] 

                    t = mqtt_topic_event + "f1_" + line.strip().split(':')[0]
                    print(t)
                    print(json.dumps(event_json))
                    client.publish(topic=t, payload=json.dumps(event_json))

    file_name = "out_by_app_f2_" + str(x) + ".csv"

    with open(file_name, "r") as ins:
        for line in ins:
            if "dump" not in line:
                if "Energy" in line:
                    energy_json["timestamp"] = start_time
                    energy_json["measurements"] = [float(line.strip().split(':')[1])]

                    t = mqtt_topic + "f2_" + "energy"
                    print(t)
                    print(json.dumps(energy_json))
                    client.publish(topic=t, payload=json.dumps(energy_json))

                else:
                    l = line.strip().split(':')
                    event = l[0]
                    d = l[1].split(',')

                    event_json["timestamp"] = int(int(d[0])/1000)
                    event_json["measurements"] = [ {"dP": float(d[1]), "dQ": float(d[2])} ] 

                    t = mqtt_topic_event + "f2_" + line.strip().split(':')[0]
                    print(t)
                    print(json.dumps(event_json))
                    client.publish(topic=t, payload=json.dumps(event_json))

    file_name = "out_by_app_f3_" + str(x) + ".csv"

    with open(file_name, "r") as ins:
        for line in ins:
            if "dump" not in line:
                if "Energy" in line:
                    energy_json["timestamp"] = start_time
                    energy_json["measurements"] = [float(line.strip().split(':')[1])]

                    t = mqtt_topic + "f3_" + "energy"
                    print(t)
                    print(json.dumps(energy_json))
                    client.publish(topic=t, payload=json.dumps(energy_json))

                else:
                    l = line.strip().split(':')
                    event = l[0]
                    d = l[1].split(',')

                    event_json["timestamp"] = int(int(d[0])/1000)
                    event_json["measurements"] = [ {"dP": float(d[1]), "dQ": float(d[2])} ] 

                    t = mqtt_topic_event + "f3_" + line.strip().split(':')[0]
                    print(t)
                    print(json.dumps(event_json))
                    client.publish(topic=t, payload=json.dumps(event_json))

    start_time = start_time + 3600


start_time = 1559001600

mqtt_topic = "saam/data/mihas/sens_power_"

energy_json = {
            "timestamp": 12345566,
            "timestep": 3600,
            "measurements": [ 234.5 ]
            }

for x in range(24):
    file_name = "out_by_app_f1_" + str(x) + ".csv"
    print(file_name)

    start_time = start_time + 3600
    print(start_time)

    with open(file_name, "r") as ins:
        for line in ins:
            if "dump" not in line:
                if "Energy" in line:
                    energy_json["timestamp"] = start_time
                    energy_json["measurements"] = [float(line.strip().split(':')[1])]
                    print(mqtt_topic + "f1_" + "energy")
                    print(energy_json)
                    print(line.strip())
                else:
                    # TODO event json
                    print(line.strip())

    file_name = "out_by_app_f2_" + str(x) + ".csv"

    with open(file_name, "r") as ins:
        for line in ins:
            if "dump" not in line:
                if "Energy" in line:
                    energy_json["timestamp"] = start_time
                    energy_json["measurements"] = [float(line.strip().split(':')[1])]
                    print(mqtt_topic + "f2_" + "energy")
                    print(energy_json)
                    print(line.strip())
                else:
                    print(line.strip())

    file_name = "out_by_app_f3_" + str(x) + ".csv"

    with open(file_name, "r") as ins:
        for line in ins:
            if "dump" not in line:
                if "Energy" in line:
                    energy_json["timestamp"] = start_time
                    energy_json["measurements"] = [float(line.strip().split(':')[1])]
                    print(mqtt_topic + "f3_" + "energy")
                    print(energy_json)
                    print(line.strip())
                else:
                    print(line.strip())

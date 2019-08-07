
start_time = 1559001600

for x in range(24):
    file_name = "out_by_app_f1_" + str(x) + ".csv"
    print(file_name)

    start_time = start_time + 3600
    print(start_time)

    with open(file_name, "r") as ins:
        for line in ins:
            if "dump" not in line:
                print(line.strip())
                # TODO if energy send energy json
                # else send event json

    file_name = "out_by_app_f2_" + str(x) + ".csv"

    with open(file_name, "r") as ins:
        for line in ins:
            if "dump" not in line:
                print(line.strip())

    file_name = "out_by_app_f3_" + str(x) + ".csv"

    with open(file_name, "r") as ins:
        for line in ins:
            if "dump" not in line:
                print(line.strip())

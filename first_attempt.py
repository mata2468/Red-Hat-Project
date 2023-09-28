import json
import os
from datetime import datetime, timedelta
from statistics import median

'''
first attempt: trying to match all data of all apps, we decided it is not convinient
and moved to another approach as in data_extractor_to_jsons.py
'''
class data_extractor:
    def extract_time(self, time):
        return datetime.strptime(time, "%Y-%m-%d %H:%M:%S") 

    def calculate_num_of_points(self, start, stop):
        return int((stop - start).total_seconds() / 60)

    def debug_print(self, start, stop, start_cpu, stop_cpu, start_mem, stop_mem, start_cpu_index, stop_cpu_index, start_mem_index, stop_mem_index, num_of_points):
        print(f'start: {start}')
        print(f'stop: {stop}')
        print(f'start_cpu: {start_cpu}')
        print(f'stop_cpu: {stop_cpu}')
        print(f'start_mem: {start_mem}')
        print(f'stop_mem: {stop_mem}')


        print(f'start_cpu_index: {start_cpu_index}')
        print(f'stop_cpu_index: {stop_cpu_index}')
        print(f'start_mem_index: {start_mem_index}')
        print(f'stop_mem_index: {stop_mem_index}')
        print(f'num_of_points: {num_of_points}')

    def __init__(self, directory="data", output_file="./points.txt", application_name="prom-label-proxy", aggregate="median", pod=""):
        # initialize arguments
        for k, v in locals().items():
            setattr(self, k, v)
        self.output_file = f"./data/{application_name}-{pod}.txt"

    def main(self):
        # Get all JSON files in the directory
        json_files = [file for file in os.listdir(self.directory) if file.endswith(".json")]

        cpu_times = [
            ('04-19_17', '04-30_16'),
            # ('04-30_16', '05-12_22'),
            # ('05-12_22', '05-28_17'),
            # ('05-28_17', '06-08_17'),
            # ('06-08_17', '06-13_21'),
        ]

        mem_times = [
            ('04-19_17', '04-30_16'),
            # ('04-30_16', '05-12_21'),
            # ('05-12_21', '05-28_14'),
            # ('05-28_14', '06-08_14'),
            # ('06-08_14', '06-13_21'),
        ]

        for ((cpu_time1, cpu_time2), (mem_time1, mem_time2)) in zip(cpu_times, mem_times):
            
            json_file = f'container_cpu_2022-{cpu_time1}_00_00_to_2022-{cpu_time2}_00_00.json'
            # json_file = 'cpu_test.json' # test
            file_path_cpu = os.path.join(self.directory, json_file)

            json_file = f'container_mem_2022-{mem_time1}_00_00_to_2022-{mem_time2}_00_00.json'
            # json_file = 'mem_test.json' # test
            file_path_mem = os.path.join(self.directory, json_file)

            with open(file_path_cpu, "r") as json_data_cpu:
                with open(file_path_mem, "r") as json_data_mem:

                    # extract cpu times
                    cpu_json = json.load(json_data_cpu)
                    keys = [key for key in cpu_json.keys() if (self.application_name in key and self.pod in key)]
                    print(f'keys: {keys}')
                    
                    times_cpu = []
                    points_cpu = []
                    for key in keys:
                        for i in range(len(cpu_json[key])):
                            times_cpu.append((self.extract_time(cpu_json[key][i]["start"]), self.extract_time(cpu_json[key][i]["stop"])))
                            points_cpu.append(cpu_json[key][i]["data"])

                    # [(start(date,hour,minute), stop(date,hour,minute)) ...]

                    # extract mem times
                    mem_json = json.load(json_data_mem)
                    keys = [key for key in mem_json.keys() if (self.application_name in key and self.pod in key)]
                    
                    times_mem = []
                    points_mem = []
                    for key in keys:
                        for i in range(len(mem_json[key])):
                            times_mem.append((self.extract_time(mem_json[key][i]["start"]), self.extract_time(mem_json[key][i]["stop"])))
                            points_mem.append(mem_json[key][i]["data"])

                    # take the intersection of the times
                    times = []
                    points = []

                    for (start_cpu, stop_cpu) , cpu_data in zip(times_cpu, points_cpu):
                        for (start_mem, stop_mem) , mem_data in zip(times_mem, points_mem):

                            start = max(start_cpu, start_mem)
                            stop = min(stop_cpu, stop_mem)

                            num_of_points = self.calculate_num_of_points(start, stop)
                            if num_of_points > 0: # the interval is valid and we want to take the points
                                
                                start_cpu_index = self.calculate_num_of_points(start_cpu, start)
                                stop_cpu_index = len(cpu_data) - self.calculate_num_of_points(stop, stop_cpu) - 1

                                start_mem_index = self.calculate_num_of_points(start_mem, start)
                                stop_mem_index = len(mem_data) - self.calculate_num_of_points(stop, stop_mem) - 1
                                
                                cpu_data = cpu_data[start_cpu_index:stop_cpu_index]
                                mem_data = mem_data[start_mem_index:stop_mem_index]

                                if len(cpu_data) == 0:
                                    assert(False)

                                if len(mem_data) == 0:
                                    assert(False)

                                if len(cpu_data) != len(mem_data):
                                    self.debug_print(start, stop, start_cpu, stop_cpu, start_mem, stop_mem, start_cpu_index, stop_cpu_index, start_mem_index, stop_mem_index, num_of_points)
                                    print(self.calculate_num_of_points(stop, stop_cpu))
                                    print(self.calculate_num_of_points(stop, stop_mem))

                                    print(f'{self.calculate_num_of_points(start_cpu, stop_cpu)}')
                                    print(f'{self.calculate_num_of_points(start_mem, stop_mem)}')

                                    assert(False)

                                for delta,(cpu_point, mem_point) in enumerate(zip(cpu_data, mem_data)):
                                    points.append((cpu_point, mem_point, start + timedelta(minutes=delta)))
                                break
                    
                    points_size = len(points)
                    aggregated_points = []
                    
                    # TODO: Add generic aggragation method
                    # aggregate the points every 30 minutes with median
                    for i in range(0, points_size, 30):
                        points_to_aggregate = points[i:i+30]
                        
                        cpu_points_to_aggregate = [point[0] for point in points_to_aggregate]
                        cpu_points_to_aggregate = median(cpu_points_to_aggregate)
                        mem_points_to_aggregate = [point[1] for point in points_to_aggregate]
                        mem_points_to_aggregate = median(mem_points_to_aggregate)
                        
                        aggregated_points.append((cpu_points_to_aggregate, mem_points_to_aggregate))

                    # export points to output file as two columns: cpu, mem
                    with open(self.output_file, 'w') as f:
                        for tuple_item in aggregated_points:
                            f.write(f"{tuple_item[0]},{tuple_item[1]}\n")


data_extractor(pod="thanos-querier-7bf6fb4c8c-jg6hk").main()
print("Data conversion complete.")

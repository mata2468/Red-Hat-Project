import matplotlib.pyplot as plt
import os
import json


class show_graphs_from_json:

    def __init__(self, application_name, is_subplot=True, is_save=False):
        self.is_subplot = is_subplot
        self.is_save = is_save
        self.application_name = application_name

    def graphs_from_json(self, file_path):
        with open(file_path, 'r') as file:
            data_json = json.load(file)

        pods = data_json.keys()
        
        for pod in pods:
            pod_data = data_json[pod]
            times = sorted(pod_data.keys())

            #TODO: make time indices readable
            cpu_points = [pod_data[time][0] for time in times] 
            mem_points = [pod_data[time][1] for time in times]

            # show graph with 2 subplots
            self.show_graph(pod, times, cpu_points, mem_points)            

    def show_graph(self, pod, times, cpu_points, mem_points):
        # Plot the first column (cpu usage)
        plt.subplot(2, 1, 1) if self.is_subplot else plt.figure(1)
        plt.plot(times, cpu_points)
        plt.xlabel('Time')
        plt.ylabel('cpu usage')
        plt.title('Graph of cpu usage')

        # Plot the second column (memory usage)
        plt.subplot(2, 1, 2) if self.is_subplot else plt.figure(2)
        plt.plot(times, mem_points)
        plt.xlabel('Time')
        plt.ylabel('memory usage')
        plt.title('Graph of memory usage')

        # Display the graphs
        plt.subplots_adjust(hspace=0.5)
        plt.suptitle(f'{self.application_name}-{pod}')
        file_name = f'{self.application_name}-{pod}'
        print(f"Saving the graph to {os.getcwd()}/graphs/{file_name}.png")
        plt.savefig(f'{os.getcwd()}\graphs\{file_name}.png') if self.is_save else plt.show()
        plt.clf()

file_path = './our_data/json\joined_data/aggregated_data/prom-label-proxy.json'
show_graphs_from_json(application_name="prom-label-proxy", is_save=True).graphs_from_json(file_path)
import matplotlib.pyplot as plt
import os


class show_graphs:

    def __init__(self, directory, is_subplot=True, is_save=False):
        self.directory = directory
        self.is_subplot = is_subplot
        self.is_save = is_save


    def main(self):
        # Get a list of all files in the directory
        all_files = os.listdir(self.directory)

        # Filter the list to include only text files
        text_files = [file for file in all_files if file.endswith('.txt')]

        for file_name in text_files:
            with open(f"./{self.directory}/{file_name}", 'r') as file:
                data = file.readlines()
                self.show_graph(data, file_name)        

    def show_graph(self, data, file_name):
        # Split the data into two separate lists
        x_values = []
        y_values = []
        for line in data:
            values = line.strip().split(',')
            x_values.append(float(values[0]))
            y_values.append(float(values[1]))

        assert len(x_values) == len(y_values)
        print('Number of points: ', len(x_values))
        print('Number of points: ', len(y_values))

        # Calculate the indices as hours (assuming 30 minutes difference between each dot)
        # for now atarting with 0 as first time, TODO: update the correct time
        indices = [i * 0.5 for i in range(len(x_values))]

        # Plot the first column (cpu usage)
        plt.subplot(2, 1, 1) if self.is_subplot else plt.figure(1)
        plt.plot(indices, x_values)
        plt.xlabel('Time(hours)')
        plt.ylabel('cpu usage')
        plt.title('Graph of cpu usage')

        # Plot the second column (memory usage)
        plt.subplot(2, 1, 2) if self.is_subplot else plt.figure(2)
        plt.plot(indices, y_values)
        plt.xlabel('Time(hours)')
        plt.ylabel('memory usage')
        plt.title('Graph of memory usage')

        # Display the graphs
        plt.subplots_adjust(hspace=0.5)
        plt.suptitle(file_name)
        print(f"Saving the graph to {os.getcwd()}/graphs/{file_name}.png")
        plt.savefig(f'{os.getcwd()}\graphs\{file_name}.png') if self.is_save else plt.show()
        plt.clf()

show_graphs(directory="./data", is_save=True).main()
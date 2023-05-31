import csv
from collections import defaultdict
import threading
from queue import Queue
import os

class Flight_Mapper(threading.Thread):
    #Thread, representing the Map stage threads
    def __init__(self, data, output_queue):
        threading.Thread.__init__(self)
        self.data = data
        self.output_queue = output_queue

    def run(self):
        # Map phase, counting the number of flights per passenger
        passenger_counts = defaultdict(int) # Create a dictionary passenger_counts with a default value of 0, which is used to count the number of flights per passenger.
        for row in self.data: # Iterate through the input data, extract the passenger ID, and add 1 to the number of flights for the passenger ID
            passenger_id = row[0]
            passenger_counts[passenger_id] += 1

        # Write (passenger ID, total number of flights) as a key-value pair to the file
        output_filename = f"Buff/mapper_output_{threading.current_thread().name}.csv"
        with open(output_filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows([(passenger_id, count) for passenger_id, count in passenger_counts.items()])

        # Put (passenger ID, total number of flights) into the output queue as an intermediate key-value pair.
        for passenger_id, count in passenger_counts.items():
            self.output_queue.put((passenger_id, count))

class Flight_Reducer(threading.Thread):
    def __init__(self, input_queue, output_queue):
        # Inherited from threading.Thread, representing the threads in the Reduce phase:
        threading.Thread.__init__(self)
        self.input_queue = input_queue
        self.output_queue = output_queue

    def run(self):
        # Reduce phase, combining the number of flights per passenger
        passenger_counts = defaultdict(int) # Create a dictionary passenger_counts with a default value of 0, which is used to merge the number of flights per passenger.
        while True:
            if self.input_queue.empty():
                break
            # Loop through the key-value pairs in the input queue and accumulate the number of flights for the passenger ID into passenger_counts.
            passenger_id, count = self.input_queue.get()
            passenger_counts[passenger_id] += count

        # Launch (passenger ID, total number of flights) as the final key-value pair
        for passenger_id, count in passenger_counts.items():
            self.output_queue.put((passenger_id, count))

def main():
    # Read CSV files
    with open('AComp_Passenger_data_no_error.csv', 'r') as f:
        passenger_data = list(csv.reader(f))

    # Create Split and Buff directories (if they don't exist)
    os.makedirs("Split", exist_ok=True)
    os.makedirs("Buff", exist_ok=True)

    # Splitting data for concurrent processing in Map phase
    num_threads = 4
    split_data = [passenger_data[i::num_threads] for i in range(num_threads)]
    mapper_threads = []
    output_queue = Queue()

    for i in range(num_threads):
        # Write the split data to a file in the Split directory
        split_filename = f"Split/split_data_{i}.csv"
        with open(split_filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(split_data[i])

        # Create and start Mapper threads
        mapper = Flight_Mapper(split_data[i], output_queue)
        mapper.start()
        mapper_threads.append(mapper)

    # Wait for all Mapper threads to finish execution.
    for thread in mapper_threads:
        thread.join()

    # Merge the output of Mapper for concurrent processing in the Reduce phase
    reducer_threads = []
    input_queue = output_queue  # Use output queues as input queues
    final_output_queue = Queue()

    for i in range(num_threads):
        # Create and start the Reducer thread
        reducer = Flight_Reducer(input_queue, final_output_queue)
        reducer.start()
        reducer_threads.append(reducer)

    for thread in reducer_threads:
        thread.join()

    # Get the output of the Reduce phase
    passenger_counts = list(final_output_queue.queue)

    # Sorted by number of rides from largest to smallest
    sorted_results = sorted(passenger_counts, key=lambda x: x[1], reverse=True)

    # Output results to file
    output_filename = "sorted_results.csv"
    with open(output_filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(sorted_results)

    # Find and print the passengers who have taken the most flights
    passenger_id, count = sorted_results[0]
    print("Passengers with the highest number of flights:")
    print(f"Passengers ID: {passenger_id}, Number of flights: {count}")


if __name__ == '__main__':
    main()

# Use python to simulate the process of mapreduce to find the passenger with the most rides.

The file **"sorted_results.csv"** holds the statistics of the number of flights taken by passengers in descending order.

And **"flight.py"** is the code to simulate mapreduce using python.

The **Buff** directory holds the output of map, while the **Split** directory holds the initial splitting of the data into several fast feeds to map.

The code has implemented a MapReduce-like process to determine the passenger with the highest number of flights. It splits the data file into multiple parts and uses the map and reduce functions to process the data and finally find the passenger who took the most number of flights.

These steps include:

Splitting the file into multiple parts: The split_file function splits the input file into multiple parts according to the specified number of splits and saves them in split_0.csv, split_1.csv, split_2.csv and split_3.csv of HDFS.

Mapping functions: The map_func function traverses each split file, extracts the passenger ID and calculates the number of flights for each passenger, and returns the key-value pairs of passenger ID and corresponding number of flights.

Buffer: Use the buffer_and_spill function to split the result of the mapping phase into smaller buffers and write the buffers to a temporary file on local disk.

Final reduction: the reduce_func function takes the list of intermediate results of the mapping phase and sums the counts of the same passenger IDs, returning each passenger ID and the corresponding total number of counts.

Read buffer data into memory: The buffer data from the temporary file is read into memory and sorted by key.

Decrease phase: Perform decrease operation on the sorted intermediate data and calculate the total number of counts for each passenger.

Write final result to file: Write the final result to the result.csv file, where each line contains the passenger ID and the total number of rides.

Print the passenger with the most number of rides: Find the passenger with the most number of flights from the final result and print the passenger ID.

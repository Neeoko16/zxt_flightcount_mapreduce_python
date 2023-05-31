# Use python to simulate the process of mapreduce to find the passenger with the most rides.

The file **"sorted_results.csv"** holds the statistics of the number of flights taken by passengers in descending order.

And **"flight.py"** is the code to simulate mapreduce using python.

The **Buff** directory holds the output of map, while the **Split** directory holds the initial splitting of the data into several fast feeds to map.

The code has implemented a MapReduce-like process to determine the passenger with the highest number of flights. It splits the data file into multiple parts and uses the map and reduce functions to process the data and finally find the passenger who took the most number of flights.


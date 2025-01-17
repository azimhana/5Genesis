from influxdb import InfluxDBClient
from random import randint
from time import sleep
from datetime import datetime

# InfluxDB connection settings
client = InfluxDBClient(host='localhost', port=8086, username='isakl', password='qwertyui', database='testDB')

# Generate and upload random data
for _ in range(10):
    timestamp = datetime.utcnow().isoformat()
    temperature = randint(0, 100)
    location = "us-midwest"

    # Create the data point
    json_body = [
        {
            "measurement": "weather",
            "tags": {
                "location": location
            },
            "time": timestamp,
            "fields": {
                "temperature": temperature
            }
        }
    ]

	

    # Write the data point to InfluxDB
    client.write_points(json_body)

    # Print a confirmation message
    print(f"Data point {temperature} added at {timestamp}")

    # Sleep for a second before generating the next data point
    sleep(1)

print("Data upload complete.")

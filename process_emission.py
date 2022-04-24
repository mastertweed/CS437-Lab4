import json
import logging
import sys

import greengrasssdk

# Logging
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# SDK Client
client = greengrasssdk.client("iot-data")

max_emission = [0,0,0,0,0]
my_counter = 0

def lambda_handler(event, context):
    global max_emission
    global my_counter
    my_counter = my_counter + 1

    logger.info(event)

    try:
        # Get data
        vehicle_id = int(str(event['vehicle_id']))
        data = float(str(event['data']))

        # Calculate max emission
        max_emission[vehicle_id] = max(max_emission[vehicle_id], data)

        # Publish all vehicles max emissions values to "emission/all"
        # This goes to the IOT console
        client.publish(
            topic="emission/all",
            payload=json.dumps(
                {"message": "Process Emission! , " + str(event['data']) + " : " + str(event['vehicle_id']),
                "count": my_counter,
                "vehicle0": max_emission[0],
                "vehicle1": max_emission[1],
                "vehicle2": max_emission[2],
                "vehicle3": max_emission[3],
                "vehicle4": max_emission[4]}
            ),
        )
        # Publish max vehicle emission value for vehicle data just recieved to "emission/all/vehicle#"
        # This goes back to the vehicle
        client.publish(
            topic="emission/all/vehicle" + str(vehicle_id),
            payload=json.dumps({"vehicle" + str(vehicle_id): max_emission[vehicle_id]}),
        )
    except Exception as e:
        client.publish(
            topic="emission/all",
            payload=json.dumps(
                {"message": "FAILED: Process Emission! , " + str(event['data']) + " : " + str(event['vehicle_id'])},
            ),
        )

    return
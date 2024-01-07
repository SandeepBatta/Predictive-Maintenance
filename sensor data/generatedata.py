import argparse
from scipy import stats
import datetime
import time
from google.cloud import pubsub_v1


def run(TOPIC_NAME, PROJECT_ID, INTERVAL=200):
    publisher = pubsub_v1.PublisherClient()

    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

    sensornames = ["Pressure_1", "Pressure_2", "Pressure_3", "Pressure_4", "Pressure_5"]
    sensorCenterLines = [1992, 2080, 2390, 1732, 1911]
    sd = [442, 388, 354, 403, 366]

    counter = 0

    while True:
        for pos in range(0, 5):
            sensor = sensornames[pos]
            reading = stats.truncnorm.rvs(
                -1, 1, loc=sensorCenterLines[pos], scale=sd[pos]
            )
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            message = timestamp + "," + sensor + "," + str(reading)
            publisher.publish(topic_path, data=message.encode("utf-8"))
            counter = counter + 1
        time.sleep(INTERVAL / 1000)
        if counter == 100:
            print("Published 100 Messages")
            counter = 0


if __name__ == "__main__":  # noqa
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--TOPIC_NAME",
        help="The Cloud Pub/Sub topic to write to.\n" '"<TOPIC_NAME>".',
    )
    parser.add_argument(
        "--PROJECT_ID",
        help="GCP Project ID.\n" '"<PROJECT_ID>".',
    )
    parser.add_argument(
        "--INTERVAL",
        type=int,
        default=200,
        help="Interval in mili seconds which will publish messages (default 2 ms).\n"
        '"<INTERVAL>"',
    )
    args = parser.parse_args()
    run(args.TOPIC_NAME, args.PROJECT_ID, args.INTERVAL)

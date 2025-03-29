from kafka import KafkaConsumer
from prometheus_client import Counter, Histogram, start_http_server
import json

# TODO: Update the Kafka topic to the movie log of your team
topic = 'movielogN'

start_http_server(9092)

# Metrics like Counter, Gauge, Histogram, Summaries
# Refer https://prometheus.io/docs/concepts/metric_types/ for details of each metric
# TODO: Define metrics to show request count. Request count is total number of requests made with a particular http status
#REQUEST_COUNT = (
#     'request_count', 'Recommendation Request Count',
#     ['http_status']
# )

REQUEST_COUNT = Counter('request_count', 'Recommendation Request Count', ['http_status'])
REQUEST_LATENCY = Histogram('request_latency_seconds', 'Request latency')


def main():
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        group_id=topic,
        enable_auto_commit=True,
        auto_commit_interval_ms=1000
    )

    for message in consumer:
        try:
            event = json.loads(message.value.decode('utf-8'))
            #print(event)  # Uncomment to see the raw event data
            if 'recommendation request' in event:
                # TODO: Increment the request count metric for the appropriate HTTP status code.
                # Hint: Extract the status code from the message and use it as a label for the metric.
                # print(values) - You can print values and see how to get the status
                status = str(event.get('status', 'unknown'))
                REQUEST_COUNT.labels(status).inc()

                # Updating request latency histogram
                time_taken = event.get('responsetime', 0)
                REQUEST_LATENCY.observe(time_taken / 1000)

                print(f"Processed message: {event}")

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON message: {e}")
        except Exception as e:
            print(f"Error processing message: {e}")


if __name__ == "__main__":
    main()

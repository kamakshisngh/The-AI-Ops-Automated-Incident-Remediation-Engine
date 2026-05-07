import json
from confluent_kafka import Consumer, KafkaError

# Configuration to connect to the broker
conf = {
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'ai-ops-monitor-group',
    'auto.offset.reset': 'earliest' # Start reading from the beginning of the topic
}

consumer = Consumer(conf)
consumer.subscribe(['server-logs'])

print("Monitoring started... Waiting for logs (Ctrl+C to stop)")

try:
    while True:
        msg = consumer.poll(1.0) # Wait for a message for up to 1 second

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}")
                break

        # Decode the JSON data
        data = json.loads(msg.value().decode('utf-8'))
        
        # Highlight 'CRITICAL' logs for visibility
        status = data['status']
        metrics = data['metrics']
        
        if status == "CRITICAL":
            print(f"🚨 [ALERT] {data['timestamp']} | CPU: {metrics['cpu_percent']}% | Latency: {metrics['latency_ms']}ms")
        else:
            print(f"✅ [NORMAL] {data['timestamp']} | CPU: {metrics['cpu_percent']}%")

except KeyboardInterrupt:
    print("\nStopping monitor...")
finally:
    consumer.close()

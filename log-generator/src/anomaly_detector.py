import json
from collections import deque
from confluent_kafka import Consumer

# Configuration
conf = {
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'ai-ops-brain-group',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(conf)
consumer.subscribe(['server-logs'])

# A sliding window to keep track of the last 20 latency readings
latency_window = deque(maxlen=20)
LATENCY_THRESHOLD = 150.0  # ms

print("🧠 AI-Ops Anomaly Detector is Online...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        
        data = json.loads(msg.value().decode('utf-8'))
        current_latency = data['metrics']['latency_ms']
        latency_window.append(current_latency)

        # Calculate the moving average
        if len(latency_window) >= 5:
            avg_latency = sum(latency_window) / len(latency_window)
            
            if avg_latency > LATENCY_THRESHOLD:
                print(f"🚨 ANOMALY DETECTED! Average Latency is {avg_latency:.2f}ms (Threshold: {LATENCY_THRESHOLD}ms)")
            else:
                print(f"🟢 System Stable. Avg Latency: {avg_latency:.2f}ms")

except KeyboardInterrupt:
    print("\nPowering down brain...")
finally:
    consumer.close()

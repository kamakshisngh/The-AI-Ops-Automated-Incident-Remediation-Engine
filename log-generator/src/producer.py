import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer

# Configuration for local connection
conf = {
    'bootstrap.servers': '127.0.0.1:9092',
    'client.id': 'server-telemetry-sensor'
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Log sent to {msg.topic()} [{msg.partition()}]")

def generate_telemetry():
    """Generates synthetic server logs with periodic 'broken' behavior."""
    timestamp = datetime.now().isoformat()
    
    # Simulate a 'broken' state every 10th message
    is_failing = random.randint(1, 10) == 10
    
    if is_failing:
        cpu_usage = random.uniform(90.0, 99.9)  # Spiking CPU
        latency = random.uniform(500, 2000)     # High latency (ms)
        status = "CRITICAL"
    else:
        cpu_usage = random.uniform(10.0, 40.0)  # Healthy CPU
        latency = random.uniform(10, 50)        # Healthy latency (ms)
        status = "OK"

    return {
        "timestamp": timestamp,
        "service": "order-processor",
        "status": status,
        "metrics": {
            "cpu_percent": round(cpu_usage, 2),
            "memory_percent": round(random.uniform(30, 60), 2),
            "latency_ms": round(latency, 2)
        }
    }

print("Starting AI-Ops Log Generator... (Ctrl+C to stop)")

try:
    while True:
        data = generate_telemetry()
        
        # Trigger the send
        producer.produce(
            'server-logs', 
            key='server-1', 
            value=json.dumps(data), 
            callback=delivery_report
        )
        
        # Serve delivery callbacks
        producer.poll(0)
        
        time.sleep(1)  # Send a log every second

except KeyboardInterrupt:
    print("\nStopping generator...")
finally:
    producer.flush()

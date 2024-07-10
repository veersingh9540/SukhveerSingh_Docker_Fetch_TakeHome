from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
import json
import os
from collections import defaultdict

# Consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

# Producer configuration
producer_conf = {
    'bootstrap.servers': 'localhost:29092'
}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)
consumer.subscribe(['user-login'])

# Dictionary to hold device type counts and a set for unique users
device_type_counts = defaultdict(int)
unique_users = set()

def process_message(data):
    # Perform some basic processing on the data
    processed_data = {
        'user_id': data.get('user_id'),
        'timestamp': data.get('timestamp'),
        'locale': data.get('locale'),
        'device_id': data.get('device_id'),
        'device_type': data.get('device_type'),
        'app_version': data.get('app_version'),
        'ip': data.get('ip')
    }
    
    # Update device type count if device_type is present
    device_type = processed_data['device_type']
    if device_type:
        device_type_counts[device_type] += 1

    # Update unique user count
    user_id = processed_data['user_id']
    unique_users.add(user_id)

    # Produce the processed data to the new Kafka topic
    producer.produce('processed-user-login', key=str(processed_data['user_id']), value=json.dumps(processed_data))
    
    return processed_data

def print_data():
    try:
        while True:
            try:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())

                data = json.loads(msg.value().decode('utf-8'))
                
                # Handle missing fields
                if not data.get('user_id'):
                    print(f"Skipping message since some of the fields are missing {data.get('user_id')}")
                    continue
                
                processed_data = process_message(data)
                
                # Print the processed data
                print("Processed Data: ", processed_data)
                
                # Print the current counts for each device type
                print(f"Current device type counts: {dict(device_type_counts)}")
                
                # Print the current unique user count
                print(f"Current unique user count: {len(unique_users)}\n")
            
            except KafkaException as e:
                print(f"Kafka error: {e}")
                continue
    
    except KeyboardInterrupt:
        menu()
    finally:
        consumer.close()
        producer.flush()


def print_filtered_data(locale_filter=None):
    try:
        while True:
            try:
                # To fetch Kafka message and 1.0 for timeout in 1 seconds
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())

                data = json.loads(msg.value().decode('utf-8'))
                
                # Handle missing fields and data
                if not data.get('user_id'):
                    print(f"Skipping message since some of the fields are missing {data.get('user_id')}")
                    continue
                
                processed_data = process_message(data)
                
                if (locale_filter and processed_data['locale'] != locale_filter):
                    continue
                
                # Print the processed data
                print("Processed Data: ", processed_data)
                
                # Print the current counts for each device type
                print(f"Current device type counts: {dict(device_type_counts)}")
                
                # Print the current unique user count
                print(f"Current unique user count: {len(unique_users)}\n")
            
            except KafkaException as e:
                print(f"Kafka error: {e}")
                continue
    
    except KeyboardInterrupt:
        menu()
    finally:
        consumer.close()
        producer.flush()

def menu():
    while True:
        print("Menu:")
        print("1. Filter by locale")
        print("2. Show All Data and user count")
        print("3. Exit (Two times you have to request for EXIT))")
        choice = input("Enter your choice: ")

        if choice == '1':
            locale_filter = input("Enter the locale to filter by: ")
            print_filtered_data(locale_filter=locale_filter)
        elif choice == '2':
            print_data()
        elif choice == '3':
            print("Exiting...")
            break
        else:
            print("Invalid choice, please try again.")

if __name__ == "__main__":
    menu()

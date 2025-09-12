from confluent_kafka.admin import AdminClient, NewTopic

def create_topic(topic_name, num_partitions, replication_factor):
    """
    Create a Kafka topic with the specified name, number of partitions, and replication factor.
    """
    admin_client = AdminClient({'bootstrap.servers':'localhost:9092'})

    existing_topics = admin_client.list_topics().topics
    if topic_name in existing_topics:
        print(f"Topic '{topic_name}' already exists.")
        return
    
    new_topic = NewTopic(topic=topic_name,
                     num_partitions=num_partitions,
                     replication_factor=replication_factor)
    fs = admin_client.create_topics([new_topic])

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic '{topic}' created")
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")

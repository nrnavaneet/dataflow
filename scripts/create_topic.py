# -*- coding: utf-8 -*-
#
# File: create_topic.py
# Description: Utility for creating Kafka topics for the stock analytics pipeline.
# This script is part of an open-source data pipeline for benchmarking and analytics.
#
# Usage: Import and call create_topic() to create a topic if not present.
# License: MIT

from confluent_kafka.admin import AdminClient, NewTopic


def create_topic(topic_name, num_partitions, replication_factor):
    """
    Create a Kafka topic with the specified name, number of partitions,
    and replication factor.

    Args:
        topic_name (str): Name of the Kafka topic to create.
        num_partitions (int): Number of partitions for the topic.
        replication_factor (int): Replication factor for the topic.

    Returns:
        None

    Prints status of topic creation or existence.
    """
    """
    Create a Kafka topic with the specified name, number of partitions,
    and replication factor.
    """
    BROKERS = "localhost:8097," "localhost:8098," "localhost:8099"

    admin_client = AdminClient({"bootstrap.servers": BROKERS})

    existing_topics = admin_client.list_topics().topics
    if topic_name in existing_topics:
        print(f"Topic '{topic_name}' already exists.")
        return

    new_topic = NewTopic(
        topic=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    )
    fs = admin_client.create_topics([new_topic])

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic '{topic}' created")
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")

import boto3

def create_kinesis_stream(stream_name, shard_count=1):
    """
    Create an AWS Kinesis Data Stream.

    :param stream_name: Name of the Kinesis stream
    :param shard_count: Number of shards (default: 1)
    """
    kinesis = boto3.client("kinesis")

    try:
        response = kinesis.create_stream(
            StreamName=stream_name,
            ShardCount=shard_count
        )
        print(f"Kinesis Data Stream '{stream_name}' created successfully.")
    except Exception as e:
        print(f"Error creating Kinesis stream: {e}")

# Example Usage:

def delete_kinesis_stream(stream_name):
    """
    Delete an AWS Kinesis Data Stream.

    :param stream_name: Name of the Kinesis stream
    """
    kinesis = boto3.client("kinesis")

    try:
        kinesis.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)
        print(f"Kinesis Data Stream '{stream_name}' deleted successfully.")
    except Exception as e:
        print(f"Error deleting Kinesis stream: {e}")

create_kinesis_stream("retail_pos_stream", 1)
#delete_kinesis_stream("retail_pos_stream")

from botocore.waiter import create_waiter_with_client
from botocore.waiter import WaiterModel
import boto3
import pyarrow.feather as feather
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("Glue")


def get_session_waiter(client, waiter_name="SessionReady", delay=3, timeout=300):
    max_attempts = timeout / delay + 1
    waiter_config = {
        "version": 2,
        "waiters": {
            "SessionReady": {
                "operation": "GetSession",
                "delay": delay,
                "maxAttempts": max_attempts,
                "acceptors": [
                    {
                        "matcher": "path",
                        "expected": "READY",
                        "argument": "Session.Status",
                        "state": "success"
                    },
                    {
                        "matcher": "path",
                        "expected": "STOPPED",
                        "argument": "Session.Status",
                        "state": "failure"
                    },
                    {
                        "matcher": "path",
                        "expected": "TIMEOUT",
                        "argument": "Session.Status",
                        "state": "failure"
                    },
                    {
                        "matcher": "path",
                        "expected": "FAILED",
                        "argument": "Session.Status",
                        "state": "failure"
                    }
                ]
            }
        }
    }
    return create_waiter_with_client(waiter_name, WaiterModel(waiter_config), client)


def get_pandas_dataframe_from_result_file(bucket, key):
    s3_client = boto3.client('s3')
    logger.debug(f"Downloading s3://{bucket}/{key}")

    s3_client.download_file(bucket, key, 'result.feather')
    pdf = feather.read_feather('result.feather')

    try:
        s3_client.delete_object(bucket, key)
    except Exception as e:
        logger.debug(f"Failed to delete s3://{bucket}/{key} due to {e}")
    return pdf


def get_columns_from_result(result):
    if result:
        return [column.get("name") for column in result.get("description")]

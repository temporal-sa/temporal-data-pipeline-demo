import dataclasses
import os

import temporalio.converter
from dotenv import load_dotenv
from temporalio.client import Client
from temporalio.envconfig import ClientConfig

from encryption_codec import EncryptionCodec

# Load environment variables from .env file
load_dotenv()

# Task queue the pipeline workflows run on. Each worker also serves a
# host-specific queue derived from it (see worker.py).
TASK_QUEUE = os.getenv("TEMPORAL_TASK_QUEUE", "data-pipeline")


async def get_client() -> Client:
    # The SDK's environment configuration reads TEMPORAL_ADDRESS,
    # TEMPORAL_NAMESPACE, TEMPORAL_API_KEY, TEMPORAL_TLS and the mTLS
    # certificate settings (or a temporal.toml profile).
    connect_config = ClientConfig.load_client_connect_config()
    connect_config.setdefault("target_host", "localhost:7233")

    data_converter = temporalio.converter.default()
    if os.getenv("ENCRYPT_PAYLOADS", "false").lower() == "true":
        print("Payloads will be encrypted")
        data_converter = dataclasses.replace(data_converter, payload_codec=EncryptionCodec())

    return await Client.connect(**connect_config, data_converter=data_converter)

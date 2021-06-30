# Consumer that binds to exclusive durable queue
# Assumes existence of queue on broker holding messages.
# Note: create queue with topic subscription 
# See https://docs.solace.com/Solace-PubSub-Messaging-APIs/API-Developer-Guide/Adding-Topic-Subscriptio.htm for more details

import os, uuid, sys
import platform
import time

from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener, ServiceEvent
from solace.messaging.resources.queue import Queue
from solace.messaging.config.retry_strategy import RetryStrategy
from solace.messaging.receiver.persistent_message_receiver import PersistentMessageReceiver
from solace.messaging.receiver.message_receiver import MessageHandler, InboundMessage
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError

from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings

if platform.uname().system == 'Windows': os.environ["PYTHONUNBUFFERED"] = "1" # Disable stdout buffer 

# upload message content to ADLS
def upload_file_to_directory(topic, payload):
    try:
        file_system_client = service_client.get_file_system_client(file_system="sample_file_system")
        #directory_client = file_system_client.create_directory(topic)
        print("\n" + f"Getting directory: {topic}")
        directory_client = file_system_client.get_directory_client(topic)
        print('\nDirectory Found')

    except:
        directory_client = file_system_client.create_directory(topic)
        print('\nDirectory Created')

    try:
        file_client = directory_client.get_file_client("sample.txt")
        print('\nFile Found')
        file_size = file_client.get_file_properties().size
        print("\n" + f"Current file size: {file_size}")
        file_client.append_data(data=payload, offset=file_size, length=len(payload))
        print('\nData Appended')
        file_client.flush_data(file_size + len(payload))
        print("\n" + f"After Append file size: {file_client.get_file_properties().size}")

    except:
        file_client = directory_client.create_file("sample.txt")
        print('\nFile Created')
        file_size = file_client.get_file_properties().size
        print("\n" + f"Current file size: {file_size}")
        file_client.append_data(data=payload, offset=0, length=len(payload))
        print('\nData Appended')
        file_client.flush_data(len(payload))
        print("\n" + f"After Append file size: {file_client.get_file_properties().size}")

# establish ADLS connection
def storeToADLS(topic, msg):
    print('\nStoring to ADLS')
    # TODO set the storage account credentials here
    storage_account_name = ''
    storage_account_key = ''
    try:  
        global service_client
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    
    except Exception as e:
        print(e)

    upload_file_to_directory(topic, msg)
    print("\n" + f"Uploaded msg to: {topic}")

# Handle received messages
class MessageHandlerImpl(MessageHandler):
    def __init__(self, receiver=None):
        self.receiver = receiver
    
    def on_message(self, message: InboundMessage):
        topic = message.get_destination_name()
        payload = message.get_payload_as_string()
        print("\n" + f"Received message on: {topic}")
        print("\n" + f"Message payload: {payload} \n")
        storeToADLS(topic, payload)
        self.receiver.ack(message)

# Inner classes for error handling
class ServiceEventHandler(ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener):
    def on_reconnected(self, e: ServiceEvent):
        print("\non_reconnected")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")
    
    def on_reconnecting(self, e: "ServiceEvent"):
        print("\non_reconnecting")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")

    def on_service_interrupted(self, e: "ServiceEvent"):
        print("\non_service_interrupted")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")

# Broker Config. Note: Could pass other properties Look into
broker_props = {
    "solace.messaging.transport.host": os.environ.get('SOLACE_HOST') or "localhost",
    "solace.messaging.service.vpn-name": os.environ.get('SOLACE_VPN') or "default",
    "solace.messaging.authentication.scheme.basic.username": os.environ.get('SOLACE_USERNAME') or "default",
    "solace.messaging.authentication.scheme.basic.password": os.environ.get('SOLACE_PASSWORD') or "default"
    }

# Build A messaging service with a reconnection strategy of 20 retries over an interval of 3 seconds
# Note: The reconnections strategy could also be configured using the broker properties object
messaging_service = MessagingService.builder().from_properties(broker_props)\
                    .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20,3))\
                    .build()

# Blocking connect thread
messaging_service.connect()
print(f'Messaging Service connected? {messaging_service.is_connected}')

# Event Handling for the messaging service
service_handler = ServiceEventHandler()
messaging_service.add_reconnection_listener(service_handler)
messaging_service.add_reconnection_attempt_listener(service_handler)
messaging_service.add_service_interruption_listener(service_handler)

# Queue name. 
# NOTE: This assumes that a persistent queue already exists on the broker with the right topic subscription 
# TODO set queue name here
queue_name = "adls"
durable_exclusive_queue = Queue.durable_exclusive_queue(queue_name)

try:
  # Build a receiver and bind it to the durable exclusive queue
  persistent_receiver: PersistentMessageReceiver = messaging_service.create_persistent_message_receiver_builder()\
            .with_message_auto_acknowledgement()\
            .build(durable_exclusive_queue)
  persistent_receiver.start()

  # Callback for received messages
  persistent_receiver.receive_async(MessageHandlerImpl(receiver=persistent_receiver))
  print(f'PERSISTENT receiver started... Bound to Queue [{durable_exclusive_queue.get_name()}]')
  try: 
      while True:
          time.sleep(1)
  except KeyboardInterrupt:
      print('\nKeyboardInterrupt received')
# Handle API exception 
except PubSubPlusClientError as exception:
  print(f'\nMake sure queue {queue_name} exists on broker!')

finally:
    if persistent_receiver and persistent_receiver.is_running():
      print('\nTerminating receiver')
      persistent_receiver.terminate(grace_period = 0)
    print('\nDisconnecting Messaging Service')
    messaging_service.disconnect()
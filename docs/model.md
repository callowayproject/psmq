Python Simple Message Queue (psmq) Model

- QueueManager
  - main entry point for developers
  - creates and manages a Connection object
  - sets up queues and their configuration with appropriate serializers and deserializers
- Connection
  - manages connection to the broker
  - provides low-level communication with the broker
- Queue
  - high-level interface for sending and receiving messages including automatic serialization and deserialization 
- ReceivedMessage
  - represents a message received from a queue
  - provides access to the message's data and metadata
- Serializer
  - a callable that takes a Python object and returns a byte string 
- Deserializer
  - a callable that takes a byte string and returns a Python object 
- psmq_library.lua
  - The Lua library that is loaded by the broker and does the actual work of queue management

Streams:

Producer (client ID, topic/channel, server)
    - produce
    - flush or poll for acks?

Consumer (group ID, auto-commit, server)
    - consume
    - commit

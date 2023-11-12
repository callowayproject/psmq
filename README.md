# Python Simple Message Queue

Python Simple Message Queue

QueueManager:

- Configured to set up a daemonized Redis server with persistent storage 
- Sentinel process to monitor queues
- creates queues
- deletes queues

Queue:

- Each queue is independent within a single Redis instance. 

- The underlying queue is run by the `pmsq_library.lua` script.

- Queue ops:

    - pop: Get the message with the highest score. The message is deleted from the queue table and the message table.

    - push: Add a message to the queue. The message is added to the message table and the queue table. Message data must be binary.

    - peek: Get a copy of the message with the highest score. The message is not deleted from the queue table or the message table.

    - delete: Delete a message from the queue. The message is deleted from the queue table and the message table.

    - get: Get the message with the highest score from the queue. The message is not deleted from the queue table or the message table. The "visibiltiy timeout" is added to the message_id's score in the queue table. 

    - Messages retrieved with `get` _must_ be deleted with `delete` or they will be returned to the queue after the visibility timeout expires. 

Message:

The result of a pop, peek, or get operation is a Message object. The Message object has the following fields:

- message_id: A unique identifier for the message. This is a sortable string based on the message's timestamp and a random string.
- sent: The timestamp when the message was sent.
- data: The message data. This is a binary string, unless the deserializer was set when getting the queue.
- first_retrieved: the timestamp when the message was first retrieved by a client. Peek operations do not update this field.
- retrieval_count: The number of times a client has retrieved the message. Peek operations do not update this field. This field is updated when the message is retrieved with `get` or `pop`.

## Basic usage

### Get or create a queue

```python
from psmq import QueueManager
from pathlib import Path

file_queues = QueueManager(db_dir=Path("~./.config/pmsq").expanduser().resolve())
file_test_queue = file_queues.get_queue("test_queue")
```

### Push a message to a queue

This example manually encodes the message to binary using msgpack and json. The message data must be binary.

```python
from psmq import QueueManager
import umsgpack
import json

memory_queues = QueueManager()
test_queue = memory_queues.get_queue("test_queue")

msgpack_data = umsgpack.packb({"foo": "bar"})
json_data = json.dumps({"foo": "bar"}).encode("utf-8")
test_queue.push(msgpack_data)
test_queue.push(json_data)
```

This example sets the default serializers and deserializers to msgpack and json. The message data can be any python object.
```python
from psmq import QueueManager
import umsgpack
import json

json_serializer = lambda x: json.dumps(x).encode("utf-8")
json_deserializer = lambda x: json.loads(x.decode("utf-8"))

memory_queues = QueueManager()
msgpack_test_queue = memory_queues.get_queue(
    "msgpack_test_queue", 
    serializer=umsgpack.packb, 
    deserializer=umsgpack.unpackb
)
json_test_queue = memory_queues.get_queue(
    "json_test_queue", 
    serializer=json_serializer, 
    deserializer=json_deserializer
)

message_data = {"foo": "bar"}
msgpack_test_queue.push(message_data)
json_test_queue.push(message_data)
```

### Pop a message from a queue

This example receives and deletes a message from the queue. If the processing of this message fails, the message is lost. 

```python
from psmq import QueueManager
import umsgpack

memory_queues = QueueManager()
test_queue = memory_queues.get_queue(
    "msgpack_test_queue", 
    serializer=umsgpack.packb, 
    deserializer=umsgpack.unpackb
)

message = test_queue.pop()
if message:
    print(message)
else:
    print("No messages in queue.")
```

### Get a message from a queue

This example receives a message from the queue. The message is not deleted from the queue. The message is deleted after successful processing. If this process crashes, the message appears in the queue after the visibility timeout expires. 

```python
from psmq import QueueManager
import umsgpack

memory_queues = QueueManager()
test_queue = memory_queues.get_queue(
    "msgpack_test_queue", 
    serializer=umsgpack.packb, 
    deserializer=umsgpack.unpackb
)

message = test_queue.get()
if message:
    print(message)
    message.delete()
else:
    print("No messages in queue.")
```




*Put a meaningful, short, plain-language description of:* 

- *what this project is trying to accomplish.*
- *why it matters.* 
- *the problem(s) this project solves.*
- *how this software can improve the lives of its audience.*
- *what sets this apart from related-projects. Linking to another doc or page is OK if this can't be expressed in a sentence or two.*

**Technology stack:** *Indicate the technological nature of the software, including primary programming language(s) and whether the software is intended as standalone or as a module in a framework or other ecosystem.*

**Status:** *Alpha, Beta, 1.1, etc. It's OK to write a sentence, too. The goal is to let interested people know where this project is at. This is also a good place to link to the CHANGELOG.md.*

**Links:** to production or demo instances


## Dependencies

*Describe any dependencies that must be installed for this software to work. This includes programming languages, databases or other storage mechanisms, build tools, frameworks, and so forth. If specific versions of other software are required, or known not to work, call that out.*

## Installation

_Detailed instructions on how to install, configure, and get the project running. This should be frequently tested to ensure reliability. Alternatively, link to a separate INSTALL document._

## Configuration

_If the software is configurable, describe it in detail, either here or in other documentation to which you link._

## Usage

_Show users how to use the software. Be specific. Use appropriate formatting when showing code snippets._

## How to test the software

_If the software includes automated tests, detail how to run those tests._

## Known issues

_Document any known significant shortcomings with the software._

## Getting help

_Instruct users how to get help with this software; this might include links to an issue tracker, wiki, mailing list, etc._


## Getting involved

_This section should detail why people should get involved and describe key areas you are currently focusing on; e.g., trying to get feedback on features, fixing certain bugs, building important pieces, etc._

_General instructions on how to contribute should be stated with a link to CONTRIBUTING.md._

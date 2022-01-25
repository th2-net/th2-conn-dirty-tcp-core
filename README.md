# Description

This is a core library for dirty TCP connections which takes care of:

* configuration via CommonFactory
* listening on MQ for messages to send
* publishing sent and received messages to MQ
* batching of published messages (by time and batch size)
* running multiples TCP connections at once
* passing TCP events and data to user-implemented handlers

# Components

* [channel](src/main/kotlin/com/exactpro/th2/conn/dirty/tcp/core/api/IChannel.kt) - represents a single TCP connection. It is used to send messages and perform connect/disconnect. Before sending message can go through handlers depending
  on [send-mode](src/main/kotlin/com/exactpro/th2/conn/dirty/tcp/core/api/IChannel.kt#L104).

* [handler](src/main/kotlin/com/exactpro/th2/conn/dirty/tcp/core/api/IProtocolHandler.kt) - main handler which handles connection events and data. Its main purpose is to split received data stream into separate messages, maintain protocol
  session and prepare outgoing messages before sending.

* [mangler](src/main/kotlin/com/exactpro/th2/conn/dirty/tcp/core/api/IProtocolMangler.kt) - secondary connection handler. Its main purpose is to mangle outgoing messages. It can also be used to send unsolicited messages and preform
  unexpected connections/disconnections.

# Send mode

Outgoing message can be handled differently depending on send mode. There are 4 following modes:

* prepare and mangle
* prepare
* mangle
* direct
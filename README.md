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

# Configuration

+ *sessions* - list of session settings
+ *ioThreads* - amount of IO threads (session-count by default)
+ *appThreads* - amount of non-IO threads (session-count * 2 by default)
+ *maxBatchSize* - max size of outgoing message batch (`1000` by default)
+ *maxFlushTime* - max message batch flush time (`1000` by default)
+ *batchByGroup* - batch messages by group instead of session alias and direction (`true` by default)
+ *publishSentEvents* - enables/disables publish of "message sent" events (`true` by default)
+ *publishConnectEvents* - enables/disables publish of "connect/disconnect" events (`true` by default)

## Session settings

+ *sessionAlias* - session alias for incoming/outgoing th2 messages
+ *sessionGroup* - session group for incoming/outgoing th2 messages
+ *handler* - handler settings
+ *mangler* - mangler settings

### Security settings

+ *ssl* - enables SSL on connection (`false` by default)
+ *sni* - enables SNI support (`false` by default)
+ *certFile* - path to server certificate (`null` by default)
+ *acceptAllCerts* - accept all server certificates (`false` by default, takes precedence over `certFile`)

**NOTE**: when using infra 1.7.0+ it is recommended to load value for `certFile` from a secret by using `${secret_path:secret_name}` syntax.

## Box configuration example

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: fix-client
spec:
  image-name: ...
  image-version: ...
  type: th2-conn
  custom-config:
    autoStart: true
    autoStopAfter: 0
    maxBatchSize: 1000
    maxFlushTime: 1000
    publishSentEvents: true
    publishConnectEvents: true
    sessions:
      - sessionAlias: client
        handler: # mangler implementation settings
          security:
            ssl: false
            sni: false
            certFile: ${secret_path:cert_secret}
            acceptAllCerts: false
          host: 127.0.0.1
          port: 4567
          maxMessageRate: 100000
          autoReconnect: true
          reconnectDelay: 5000
        mangler: ... # handler implementation settings
  pins:
    - name: to_send
      connection-type: mq
      attributes:
        - subscribe
        - send
        - raw
      settings:
        storageOnDemand: false
        queueLength: 1000
    - name: outgoing_messages
      connection-type: mq
      attributes:
        - second
        - publish
        - raw
    - name: incoming_messages
      connection-type: mq
      attributes:
        - first
        - publish
        - raw
  extended-settings:
    externalBox:
      enabled: false
    service:
      enabled: false
    resources:
      limits:
        memory: 500Mi
        cpu: 1000m
      requests:
        memory: 100Mi
        cpu: 100m
```

# Changelog

## 2.1.0
* vulnerabilities check stage
* reusable workflow usage
* vulnerabilities fix

## 2.0.2

* add channel flush timeout

## 2.0.1

* use separate executor for handling sent messages to avoid reordering

## 2.0.0

* offload channel management to handler
* allow handler to handle multiple channels
* add support for session groups

## 1.0.0

* allow mangler to update metadata
* perform handle-mangle-send sequence automatically
* perform reconnect asynchronously
* event batching
* SNI support via `security.sni` option
* ability load server certificate from file via `security.certFile` option
* ability to accept all server certificates via `security.acceptAllCerts` option
* sending is throttled by network buffer instead of sending acknowledgement
* reconnect can be disabled via `autoReconnect` option
* reconnect events can be disabled via `publishConnectEvents` option
* per-session message rate throttling via `maxMessageRate` option

## 0.0.5

* use scheduler for reconnect tasks

## 0.0.4

* use existing root event (if any)

## 0.0.3

* publish error event lazily

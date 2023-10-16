# th2-conn-dirty-tcp-core (3.2.0)

This is a core library for dirty TCP connections which takes care of:

* configuration via CommonFactory
* listening on protobuf or th2 transport MQs for messages to send
* publishing sent and received messages to MQ using either of protobuf, th2 transport protocols
* batching of published messages (by time and batch size)
* running multiples TCP connections at once
* passing TCP events and data to user-implemented handlers

You can read th2 transport protocol specification by
the [link](https://exactpro.atlassian.net/wiki/spaces/TH2/pages/1048838145/TH2+Transport+Protocol)

# Components

* [channel](src/main/kotlin/com/exactpro/th2/conn/dirty/tcp/core/api/IChannel.kt) - represents a single TCP connection.
  It is used to send messages and perform connect/disconnect. Before sending message can go through handlers depending
  on [send-mode](src/main/kotlin/com/exactpro/th2/conn/dirty/tcp/core/api/IChannel.kt#L120).

* [handler](src/main/kotlin/com/exactpro/th2/conn/dirty/tcp/core/api/IHandler.kt) - main handler which handles
  connection events and data. Its main purpose is to split received data stream into separate messages, maintain
  protocol
  session and prepare outgoing messages before sending.

* [mangler](src/main/kotlin/com/exactpro/th2/conn/dirty/tcp/core/api/IMangler.kt) - secondary connection handler. Its
  main purpose is to mangle outgoing messages. It can also be used to send unsolicited messages and preform
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
+ *sendLimit* - global send limit in bytes (`0` by default which means no limit)
+ *receiveLimit* - global receive limit in bytes (`0` by default which means no limit)
+ *useTransport* - use th2 transport or protobuf protocol to publish incoming/outgoing messages (`false` by default)

## Session settings

+ *sessionGroup* - session group for incoming/outgoing th2 messages (equal to session alias by default)
+ *sessionAlias* - session alias for incoming/outgoing th2 messages
+ *handler* - handler settings
+ *mangler* - mangler settings (`null` by default)

**NOTE**: if mangler settings are `null` then no mangling will be done

### Security settings

+ *ssl* - enables SSL on connection (`false` by default)
+ *sni* - enables SNI support (`false` by default)
+ *certFile* - path to server certificate (`null` by default)
+ *acceptAllCerts* - accept all server certificates (`false` by default, takes precedence over `certFile`)

**NOTE**: when using infra 1.7.0+ it is recommended to load value for `certFile` from a secret by
using `${secret_path:secret_name}` syntax.

## Built-in mangler

This library also contains a basic mangler which will be used if library user does not implement its own.

### Configuration

+ *rules* - list of mangling rules

#### Rule

```yaml
name: String # rule name
if-contains: List<ValueSelector> # list of values expected in a message for this rule to be applied
then: List<Action> # list of actions to be applied to a matching message
```

#### Value definition

```yaml
i8: Byte # defines 8-bit integer value
i16: Short # defines 16-bit integer value
i16be: Short # defines big-endian 16-bit integer value
i32: Int # defines 32-bit integer value
i32be: Int # defines big-endian 32-bit integer value
i64: Long # defines 64-bit integer value
i64be: Long # defines big-endian 64-bit integer value
f32: Float # defines 32-bit float value
f32be: Float # defines big-endian 32-bit float value
f64: Double # defines 64-bit double value
f64be: Double # defines big-endian 64-bit double value
str: String # defines string value
bytes: Byte # defines byte-array value
charset: Charset # charset of a string value ("UTF_8" by default)
```

**NOTE**: only single value type can be specified in a definition

#### Value selector definition

Selector defines a single expected value at a specified or arbitrary position in a message.  
It is defined the same way as a value except that it can also contain an offset at which value will be expected:

```yaml
at-offset: Int # value offset (-1 by default)
```

**NOTE**: negative or absent offset means that value has no predefined offset in a message

#### Action definitions:

* set - sets a value at a specified position:

  ```yaml
  set: ValueSelector # value to set
  ```

  **NOTE**: value selector must contain value offset


* add - adds a value at a specified position or before/after existing value:

  ```yaml
  add: ValueSelector # value to add
  before: ValueSelector # value before which this value will be added (optional)
  after: ValueSelector # value after which this value will be added (optional)
  ```

  **NOTE**: `add` selector cannot have an offset if `before` or `after` selector is specified and vice versa


* move - moves a value after or before another value

  ```yaml
  move: ValueSelector # value to move
  before: ValueSelector # value before which this value will be placed
  after: ValueSelector # value after which this value will be placed
  ```

* replace - replaces a value with another value

  ```yaml
  replace: ValueSelector # value to replace
  with: ValueDefinition # value to replace this value with
  ```
* remove - removes a value

  ```yaml
  remove: ValueSelector # value to remove
  ```

### Message will be mangled when:

1) It contains all values from `if-contains` list of a rule from mangler configuration
2) It contains existing rule name in `rule-name` property - in this case rule will be applied unconditionally
3) It contains rule actions in YAML format in `rule-actions` property - specified actions will be applied

### Mangler configuration example

```yaml
rules:
  - name: replace_login_username
    if-contains:
      - i8: 1 # login message type
        at-offset: 2 # message type offset
    then:
      - replace:
          str: old_username # username value
          at-offset: 10 # username offset
        with:
          str: new_username        
```

## Box configuration example

* least one of `to_send_via_protobuf` or `to_send_via_transport` pins is required, it's mean that conn can consume
  messages via one or both protocols,
  but ability to process depends on handler implementation (please clarify in implementation README)
* `processed_messages_via_protobuf` pin are required when useTransport is `false`
* `processed_messages_via_transport` pin are required when useTransport is `true`

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
    - name: to_send_via_protobuf
      connection-type: mq
      attributes:
        - subscribe
        - send
        - raw
      settings:
        storageOnDemand: false
        queueLength: 1000
    - name: to_send_via_transport
      connection-type: mq
      attributes:
        - subscribe
        - send
        - transport-group
      settings:
        storageOnDemand: false
        queueLength: 1000
    - name: processed_messages_via_protobuf
      connection-type: mq
      attributes:
        - publish
        - store
        - raw
    - name: processed_messages_via_transport
      connection-type: mq
      attributes:
        - publish
        - transport-group
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

## 3.2.2

* Add `th2.operation_timestamp` property to a message.
  It contains the send/receive operation timestamp in ISO format: `2023-10-16T09:21:12.178299Z`

## 3.2.1
* Avoid messages loss in case of failures while saving mangler events.

## 3.2.0

* updated bom: `4.5.0-dev`
* updated common: `5.4.0-dev`
* updated common-utils: `2.2.0-dev`
* updated kotlin: `1.8.22`

## 3.1.0

* add support for th2 transport protocol
* migrated to message batcher from common-utils
* th2-common updated to `5.3.2-dev`
* th2-common-utils added `2.1.1-dev` version

## 3.0.0

* add support for session groups, books and pages

## 2.2.2

* fix `move` action in mangler not being marked as applied

## 2.2.1

* th2-common upgrade to `3.44.1`
* th2-bom upgrade to `4.2.0`

## 2.2.0

* add basic mangler

## 2.1.0

* allow to retrieve gRPC service from handler context
* support JSR-310 date and time types in settings

## 2.0.5

* add option to set global send/receive limit

## 2.0.4

* disable mangling if no mangler settings are specified

## 2.0.3

* bump `common` dependency to `3.44.0`
* bump `common-utils` dependency to `0.0.3`

## 2.0.2

* add channel flush timeout

## 2.0.1

* use separate executor for handling sent messages to avoid reordering

## 2.0.0

* offload channel management to handler
* allow handler to handle multiple channels
* session groups support preparations

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

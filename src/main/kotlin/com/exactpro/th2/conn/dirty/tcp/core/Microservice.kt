/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.conn.dirty.tcp.core

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.direction
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.message.sessionGroup
import com.exactpro.th2.common.schema.dictionary.DictionaryType
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.QueueAttribute
import com.exactpro.th2.common.schema.message.QueueAttribute.EVENT
import com.exactpro.th2.common.schema.message.QueueAttribute.PUBLISH
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction.OUTGOING
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.common.utils.event.transport.toProto
import com.exactpro.th2.common.utils.message.RAW_DIRECTION_SELECTOR
import com.exactpro.th2.common.utils.message.RAW_GROUP_SELECTOR
import com.exactpro.th2.common.utils.message.sessionAlias
import com.exactpro.th2.common.utils.message.transport.MessageBatcher.Companion.ALIAS_SELECTOR
import com.exactpro.th2.common.utils.message.transport.MessageBatcher.Companion.GROUP_SELECTOR
import com.exactpro.th2.common.utils.message.transport.toProto
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandler
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandlerFactory
import com.exactpro.th2.conn.dirty.tcp.core.api.IManglerFactory
import com.exactpro.th2.conn.dirty.tcp.core.api.impl.HandlerContext
import com.exactpro.th2.conn.dirty.tcp.core.api.impl.ManglerContext
import com.exactpro.th2.conn.dirty.tcp.core.api.impl.mangler.NoOpMangler
import com.exactpro.th2.conn.dirty.tcp.core.util.eventId
import com.exactpro.th2.conn.dirty.tcp.core.util.logId
import com.exactpro.th2.conn.dirty.tcp.core.util.messageId
import com.exactpro.th2.conn.dirty.tcp.core.util.sessionAlias
import com.exactpro.th2.conn.dirty.tcp.core.util.storeEvent
import com.exactpro.th2.conn.dirty.tcp.core.util.toErrorEvent
import com.exactpro.th2.conn.dirty.tcp.core.util.toEvent
import com.exactpro.th2.conn.dirty.tcp.core.util.toProtoRawMessageBuilder
import com.exactpro.th2.conn.dirty.tcp.core.util.toTransportRawMessageBuilder
import io.netty.buffer.ByteBuf
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.handler.traffic.GlobalTrafficShapingHandler
import mu.KotlinLogging
import java.io.InputStream
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.SECONDS
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup as TransportMessageGroup
import com.exactpro.th2.common.utils.message.RawMessageBatcher as ProtoMessageBatcher
import com.exactpro.th2.common.utils.message.transport.MessageBatcher as TransportMessageBatcher

class Microservice(
    private val defaultRootEventID: EventID,
    private val settings: Settings,
    private val readDictionary: (DictionaryType) -> InputStream,
    private val eventRouter: MessageRouter<EventBatch>,
    private val protoMessageRouter: MessageRouter<MessageGroupBatch>,
    private val transportMessageRouter: MessageRouter<GroupBatch>,
    private val handlerFactory: IHandlerFactory,
    private val manglerFactory: IManglerFactory,
    private val grpcRouter: GrpcRouter,
    private val sessionAliasToBook: Map<String, String>,
    private val rootEventsByBook: Map<String, EventID>,
    private val registerResource: (name: String, destructor: () -> Unit) -> Unit,
) {
    private val errorEventIdsByBook = ConcurrentHashMap<String, EventID>()
    private val groupEventIds = ConcurrentHashMap<String, EventID>()
    private val sessionEventIds = ConcurrentHashMap<String, EventID>()
    private val defaultBookName: String = defaultRootEventID.bookName

    private val executor = Executors.newScheduledThreadPool(settings.appThreads + settings.ioThreads).apply {
        registerResource("executor") {
            shutdown()

            if (!awaitTermination(5, SECONDS)) {
                K_LOGGER.warn { "Failed to shutdown executor in 5 seconds" }
                shutdownNow()
            }
        }
    }

    private val eventLoopGroup = NioEventLoopGroup(settings.ioThreads, executor).apply {
        registerResource("event-loop-group") {
            shutdownGracefully().syncUninterruptibly()
        }
    }

    private val shaper = GlobalTrafficShapingHandler(executor, settings.sendLimit, settings.receiveLimit).apply {
        registerResource("traffic-shaper", ::release)
    }

    private val eventBatcher = EventBatcher(
        maxBatchSizeInItems = settings.maxBatchSize,
        maxFlushTime = settings.maxFlushTime,
        executor = executor
    ) { batch ->
        eventRouter.sendAll(batch, PUBLISH.toString(), EVENT.toString())
    }.apply {
        registerResource("event-batcher", ::close)
    }

    private val handlers = ConcurrentHashMap<String, MutableMap<String, IHandler>>()

    private val channelFactory: ChannelFactory

    init {
        val messageAcceptor = if (settings.useTransport) {
            val messageBatcher = TransportMessageBatcher(
                settings.maxBatchSize,
                settings.maxFlushTime,
                defaultBookName,
                if (settings.batchByGroup) GROUP_SELECTOR else ALIAS_SELECTOR,
                executor
            ) { batch ->
                transportMessageRouter.send(batch)
                publishSentEvents(batch)
            }.apply {
                registerResource("transport-message-batcher", ::close)
            }

            fun(buff: ByteBuf, messageId: MessageID, metadata: Map<String, String>, eventId: EventID?) {
                messageBatcher.onMessage(
                    buff.toTransportRawMessageBuilder(messageId, metadata, eventId), messageId.connectionId.sessionGroup, sessionAliasToBook[messageId.sessionAlias]
                )
            }
        } else {
            val messageBatcher = ProtoMessageBatcher(
                settings.maxBatchSize,
                settings.maxFlushTime,
                if (settings.batchByGroup) RAW_GROUP_SELECTOR else RAW_DIRECTION_SELECTOR,
                executor
            ) { batch ->
                protoMessageRouter.send(batch, QueueAttribute.RAW.value)
                publishSentEvents(batch)
            }.apply {
                registerResource("proto-message-batcher", ::close)
            }

            fun(buff: ByteBuf, messageId: MessageID, metadata: Map<String, String>, eventId: EventID?) {
                messageBatcher.onMessage(buff.toProtoRawMessageBuilder(messageId, metadata, eventId))
            }
        }

        channelFactory = ChannelFactory(
            executor,
            eventLoopGroup,
            shaper,
            eventBatcher::onEvent,
            messageAcceptor,
            eventRouter::storeEvent,
            settings.publishConnectEvents
        )

        settings.sessions.forEach(::initSession)
    }

    fun run() {
        handlers.forEach { (group, handlers) ->
            handlers.forEach { (session, handler) ->
                runCatching(handler::onStart).onFailure {
                    throw IllegalStateException("Failed to start handler: $group/$session", it)
                }
            }
        }

        val proto = runCatching {
            checkNotNull(protoMessageRouter.subscribe(::handleBatch, INPUT_QUEUE_ATTRIBUTE, RAW_QUEUE_ATTRIBUTE))
        }.onSuccess { monitor ->
            registerResource("proto-raw-monitor", monitor::unsubscribe)
        }.onFailure {
            K_LOGGER.warn(it) { "Failed to subscribe to input protobuf queue" }
        }

        val transport = runCatching {
            checkNotNull(transportMessageRouter.subscribe(::handleBatch, INPUT_QUEUE_ATTRIBUTE, TRANSPORT_QUEUE_ATTRIBUTE))
        }.onSuccess { monitor ->
            registerResource("transport-raw-monitor", monitor::unsubscribe)
        }.onFailure {
            K_LOGGER.warn(it) { "Failed to subscribe to input transport queue" }
        }

        if (proto.isFailure && transport.isFailure) {
            error("Subscribe pin should be declared at least one of protobuf or transport protocols")
        }
    }

    @Suppress("UNUSED_PARAMETER")
    private fun handleBatch(metadata: DeliveryMetadata, batch: MessageGroupBatch) {
        batch.groupsList.forEach { group ->
            group.runCatching(::handleGroup).recoverCatching { cause ->
                onError("Failed to handle protobuf message group", group, cause)
            }
        }
    }

    private fun handleGroup(group: MessageGroup) {
        if (group.messagesCount != 1) {
            onError("Protobuf message group must contain only a single message", group)
            return
        }

        val message = group.messagesList[0]

        if (!message.hasRawMessage()) {
            onError("Protobuf message is not a raw message", message)
            return
        }

        val rawMessage = message.rawMessage

        rawMessage.eventId?.run {
            if (bookName != sessionAliasToBook[rawMessage.sessionAlias]) {
                onError("Unexpected book name: $bookName (expected: ${sessionAliasToBook[rawMessage.sessionAlias]})", message)
                return
            }
        }

        val sessionAlias = rawMessage.sessionAlias
        val sessionGroup = rawMessage.sessionGroup.ifBlank { sessionAlias }

        val handler = channelFactory.getHandler(sessionGroup, sessionAlias) ?: run {
            onError("Unknown session group or alias: $sessionGroup/$sessionAlias", message)
            return
        }

        handler.send(rawMessage).exceptionally {
            onError("Failed to send protobuf message", message, it)
            null
        }
    }

    @Suppress("UNUSED_PARAMETER")
    private fun handleBatch(metadata: DeliveryMetadata, batch: GroupBatch) {
        batch.groups.forEach { group ->
            group.runCatching {
                handleGroup(group, batch.book, batch.sessionGroup)
            }.recoverCatching { cause ->
                onError("Failed to handle transport message group", group, batch.book, batch.sessionGroup, cause)
            }
        }
    }

    private fun handleGroup(group: TransportMessageGroup, book: String, sessionGroup: String) {
        if (group.messages.size != 1) {
            onError("Transport message group must contain only a single message", group, book, sessionGroup)
            return
        }

        val message = group.messages[0]

        if (message !is RawMessage) {
            onError("Transport message is not a raw message", message, book, sessionGroup)
            return
        }

        message.eventId?.run {
            val expectedBookName = sessionAliasToBook[message.id.sessionAlias]
            if(book != expectedBookName) {
                onError(
                    "Unexpected book name: ${this.book} (expected: ${expectedBookName})",
                    message,
                    book,
                    sessionGroup
                )
                return
            }
        }

        val sessionAlias = message.id.sessionAlias
        val resolvedSessionGroup = sessionGroup.ifBlank { sessionAlias }

        val handler = channelFactory.getHandler(resolvedSessionGroup, sessionAlias) ?: run {
            onError(
                "Unknown session group or alias: $resolvedSessionGroup/$sessionAlias",
                message,
                book,
                resolvedSessionGroup
            )
            return
        }

        handler.send(message).exceptionally {
            onError("Failed to send transport message", message, book, resolvedSessionGroup, it)
            null
        }
    }

    private fun initSession(session: SessionSettings) {
        val sessionGroup = session.sessionGroup
        val sessionAlias = session.sessionAlias

        val groupEventId = groupEventIds.getOrPut(sessionGroup) {
            eventRouter.storeEvent("Group: $sessionGroup".toEvent(), rootEventsByBook[session.bookName] ?: defaultRootEventID)
        }

        val sessionEventId = sessionEventIds.getOrPut(sessionAlias) {
            eventRouter.storeEvent("Session: $sessionAlias".toEvent(), groupEventId)
        }

        val sendEvent: (Event) -> Unit = { onEvent(it, sessionEventId) }

        val handlerContext = HandlerContext(
            session.handler,
            session.bookName ?: defaultBookName,
            sessionAlias,
            channelFactory,
            readDictionary,
            sendEvent
        ) { clazz -> grpcRouter.getService(clazz) }
        val handler = handlerFactory.create(handlerContext)

        val mangler = when (val settings = session.mangler) {
            null -> NoOpMangler
            else -> manglerFactory.create(ManglerContext(settings, readDictionary, sendEvent))
        }

        channelFactory.registerSession(sessionGroup, sessionAlias, handler, mangler, sessionEventId)

        registerResource("handler-$sessionAlias", handler::close)
        registerResource("mangler-$sessionAlias", mangler::close)

        handlers.getOrPut(sessionGroup, ::ConcurrentHashMap)[sessionAlias] = handler
    }

    private fun publishSentEvents(batch: MessageGroupBatch) {
        if (!settings.publishSentEvents) return

        publishSentEvent(hashMapOf<EventID, MutableList<MessageID>>().apply {
            for (group in batch.groupsList) {
                val message = group.messagesList[0].rawMessage
                val eventId = message.eventId ?: continue
                if (message.direction != SECOND) continue
                getOrPut(eventId, ::ArrayList) += message.metadata.id
            }
        })
    }

    private fun publishSentEvents(batch: GroupBatch) {
        if (!settings.publishSentEvents) return

        publishSentEvent(hashMapOf<EventID, MutableList<MessageID>>().apply {
            for (group in batch.groups) {
                val message = group.messages[0]
                val eventId = message.eventId ?: continue
                if (message.id.direction != OUTGOING) continue
                getOrPut(eventId.toProto(), ::ArrayList) += message.id.toProto(batch.book, batch.sessionGroup)
            }
        })
    }

    private fun publishSentEvent(eventMessages: HashMap<EventID, MutableList<MessageID>>) {
        for ((eventId, messageIds) in eventMessages) {
            val event = "Sent ${messageIds.size} message(s)".toEvent()
            messageIds.forEach(event::messageID)
            onEvent(event, eventId)
        }
    }

    private fun onEvent(event: Event, parentId: EventID) {
        eventBatcher.onEvent(event.toProto(parentId))
    }

    private fun onError(error: String, message: AnyMessage, cause: Throwable? = null) {
        onError(error, cause, message.messageId, message.getErrorEventId())
    }

    private fun onError(
        error: String,
        message: Message<*>,
        book: String,
        sessionGroup: String,
        cause: Throwable? = null
    ) {
        onError(error, cause, message.id.toProto(book, sessionGroup), message.getErrorEventId())
    }

    private fun onError(error: String, cause: Throwable?, id: MessageID, parentEventId: EventID) {
        val event = error.toErrorEvent(cause).messageID(id)
        K_LOGGER.error("$error (message: ${id.logId})", cause)
        onEvent(event, parentEventId)
    }


    private fun onError(error: String, group: MessageGroup, cause: Throwable? = null) {
        val messageIds = group.messagesList.groupBy(
            { it.getErrorEventId() },
            { it.messageId }
        )

        onError(cause, error, messageIds)
    }

    private fun onError(
        error: String,
        group: TransportMessageGroup,
        book: String,
        sessionGroup: String,
        cause: Throwable? = null
    ) {
        val messageIds = group.messages.groupBy(
            { it.getErrorEventId() },
            { it.id.toProto(book, sessionGroup) }
        )

        onError(cause, error, messageIds)
    }

    private fun onError(cause: Throwable?, error: String, messageIds: Map<EventID, List<MessageID>>) {
        K_LOGGER.error(cause) { "$error (messages: ${messageIds.values.flatten().map(MessageID::logId)})" }

        messageIds.forEach { (parentEventId, messageIds) ->
            val event = error.toErrorEvent(cause)
            messageIds.forEach(event::messageID)
            onEvent(event, parentEventId)
        }
    }

    private fun AnyMessage.getErrorEventId(): EventID {
        return eventId ?: sessionEventIds[sessionAlias] ?: errorEventIdBySessionAlias(sessionAlias)
    }

    private fun Message<*>.getErrorEventId(): EventID {
        return eventId?.toProto() ?: sessionEventIds[id.sessionAlias] ?: errorEventIdBySessionAlias(id.sessionAlias)
    }

    private fun errorEventIdBySessionAlias(sessionAlias: String): EventID {
        val book = sessionAliasToBook[sessionAlias]
        return errorEventIdsByBook.getOrPut(book) {
            eventRouter.storeEvent(
                "Errors".toErrorEvent(),
                rootEventsByBook[book] ?: defaultRootEventID
            )
        }
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}

        private const val INPUT_QUEUE_ATTRIBUTE = "send"
        private const val RAW_QUEUE_ATTRIBUTE = "raw"
        private const val TRANSPORT_QUEUE_ATTRIBUTE = "transport-group"
    }
}
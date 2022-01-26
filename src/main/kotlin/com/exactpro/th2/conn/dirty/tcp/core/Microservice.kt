/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.direction
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.schema.dictionary.DictionaryType
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.QueueAttribute
import com.exactpro.th2.common.schema.message.storeEvent
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolHandlerFactory
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolManglerFactory
import com.exactpro.th2.conn.dirty.tcp.core.api.impl.Channel
import com.exactpro.th2.conn.dirty.tcp.core.api.impl.Context
import com.exactpro.th2.conn.dirty.tcp.core.util.eventId
import com.exactpro.th2.conn.dirty.tcp.core.util.logId
import com.exactpro.th2.conn.dirty.tcp.core.util.messageId
import com.exactpro.th2.conn.dirty.tcp.core.util.sessionAlias
import com.exactpro.th2.conn.dirty.tcp.core.util.toErrorEvent
import com.exactpro.th2.conn.dirty.tcp.core.util.toEvent
import io.netty.channel.nio.NioEventLoopGroup
import mu.KotlinLogging
import java.io.InputStream
import java.net.InetSocketAddress
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.SECONDS

class Microservice(
    rootEventId: String?,
    private val settings: Settings,
    private val readDictionary: (DictionaryType) -> InputStream,
    private val eventRouter: MessageRouter<EventBatch>,
    private val messageRouter: MessageRouter<MessageGroupBatch>,
    private val handlerFactory: IProtocolHandlerFactory,
    private val manglerFactory: IProtocolManglerFactory,
    private val registerResource: (name: String, destructor: () -> Unit) -> Unit,
) {
    private val logger = KotlinLogging.logger {}

    private val rootEventId = rootEventId ?: eventRouter.storeEvent("Dirty TCP client".toEvent()).id
    private val errorEventId by lazy { eventRouter.storeEvent("Errors".toErrorEvent(), rootEventId).id }

    private val executor = Executors.newScheduledThreadPool(settings.totalThreads).apply {
        registerResource("executor") {
            shutdown()

            if (!awaitTermination(5, SECONDS)) {
                logger.warn { "Failed to shutdown executor in 5 seconds" }
                shutdownNow()
            }
        }
    }

    private val eventLoopGroup = NioEventLoopGroup(settings.ioThreads, executor).apply {
        registerResource("event-loop-group") {
            shutdownGracefully().syncUninterruptibly()
        }
    }

    private val sequencePool = TaskSequencePool(executor).apply {
        registerResource("stream-executor", ::close)
    }

    private val incomingBatcher = MessageBatcher(settings.maxBatchSize, settings.maxFlushTime, executor) { batch ->
        messageRouter.send(batch, QueueAttribute.FIRST.value)
    }.apply {
        registerResource("incoming-batcher", ::close)
    }

    private val outgoingBatcher = MessageBatcher(settings.maxBatchSize, settings.maxFlushTime, executor) { batch ->
        messageRouter.send(batch, QueueAttribute.SECOND.value)
        publishSentEvents(batch)
    }.apply {
        registerResource("outgoing-batcher", ::close)
    }

    private val channels = settings.sessions.associateBy(SessionSettings::sessionAlias, ::createChannel)
    private val manager = ChannelManager(channels, executor)

    fun run() {
        runCatching {
            checkNotNull(messageRouter.subscribe(::handleBatch, INPUT_QUEUE_ATTRIBUTE))
        }.onSuccess { monitor ->
            registerResource("raw-monitor", monitor::unsubscribe)
        }.onFailure {
            throw IllegalStateException("Failed to subscribe to input queue", it)
        }

        if (settings.autoStart) manager.openAll(settings.autoStopAfter)
    }

    private fun handleBatch(tag: String, batch: MessageGroupBatch) {
        batch.groupsList.forEach { group ->
            group.runCatching(::handleGroup).recoverCatching { cause ->
                onError("Failed to handle message group", group, cause)
            }
        }
    }

    private fun handleGroup(group: MessageGroup) {
        if (group.messagesCount != 1) {
            onError("Message group must contain only a single message", group)
            return
        }

        val message = group.messagesList[0]

        if (!message.hasRawMessage()) {
            onError("Message is not a raw message", message)
            return
        }

        val rawMessage = message.rawMessage
        val sessionAlias = rawMessage.sessionAlias

        if (sessionAlias !in channels) {
            onError("Unknown session alias: $sessionAlias", message)
            return
        }

        sequencePool["send-$sessionAlias"].execute {
            val client = manager.open(sessionAlias, settings.autoStopAfter)

            rawMessage.runCatching(client::send).recoverCatching { cause ->
                onError("Failed to send message", message, cause)
            }
        }
    }

    private fun createChannel(session: SessionSettings): Channel {
        val sessionAlias = session.sessionAlias
        val parentEventId = eventRouter.storeEvent(sessionAlias.toEvent(), rootEventId).id
        val sendEvent: (Event) -> Unit = { eventRouter.storeEvent(it, parentEventId) }

        val handlerContext = Context(session.handler, readDictionary, sendEvent)
        val handler = handlerFactory.create(handlerContext)

        val manglerContext = Context(session.mangler, readDictionary, sendEvent)
        val mangler = manglerFactory.create(manglerContext)

        val channel = Channel(
            InetSocketAddress(session.host, session.port),
            session.secure,
            sessionAlias,
            settings.reconnectDelay,
            handler,
            mangler,
            ::onEvent,
            ::onMessage,
            eventLoopGroup,
            sequencePool,
            EventID.newBuilder().setId(parentEventId).build()
        )

        handlerContext.channel = channel
        manglerContext.channel = channel

        registerResource("channel-$sessionAlias", channel::close)
        registerResource("handler-$sessionAlias", handler::close)
        registerResource("mangler-$sessionAlias", mangler::close)

        sequencePool.create("send-$sessionAlias")

        return channel
    }

    private fun publishSentEvents(batch: MessageGroupBatch) {
        if (!settings.publishSentEvents) return

        val eventMessages = batch.groupsList.groupBy(
            { it.messagesList[0].eventId },
            { it.messagesList[0].messageId }
        )

        for ((eventId, messageIds) in eventMessages) {
            if (eventId == null) continue
            val event = "Sent ${messageIds.size} message(s)".toEvent()
            messageIds.forEach(event::messageID)
            eventRouter.storeEvent(event, eventId.id)
        }
    }

    private fun onEvent(event: Event, parentEventId: EventID) {
        eventRouter.storeEvent(event, parentEventId.id)
    }

    private fun onMessage(message: RawMessage) = when (message.direction) {
        FIRST -> incomingBatcher.onMessage(message)
        else -> outgoingBatcher.onMessage(message)
    }

    private fun onError(error: String, message: AnyMessage, cause: Throwable? = null) {
        val id = message.messageId
        val event = error.toErrorEvent(cause).messageID(id)
        logger.error("$error (message: ${id.logId})", cause)
        eventRouter.storeEvent(event, message.getErrorEventId())
    }

    private fun onError(error: String, group: MessageGroup, cause: Throwable? = null) {
        val messageIds = group.messagesList.groupBy(
            { it.getErrorEventId() },
            { it.messageId }
        )

        logger.error(cause) { "$error (messages: ${messageIds.values.flatten().map(MessageID::logId)})" }

        messageIds.forEach { (parentEventId, messageIds) ->
            val event = error.toErrorEvent(cause)
            messageIds.forEach(event::messageID)
            eventRouter.storeEvent(event, parentEventId)
        }
    }

    private fun AnyMessage.getErrorEventId(): String {
        return (eventId ?: channels[sessionAlias]?.parentEventId)?.id ?: errorEventId
    }

    companion object {
        private const val INPUT_QUEUE_ATTRIBUTE = "send"
    }
}
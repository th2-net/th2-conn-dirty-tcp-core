/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.event.EventUtils.toEventID
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
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.QueueAttribute
import com.exactpro.th2.common.schema.message.QueueAttribute.EVENT
import com.exactpro.th2.common.schema.message.QueueAttribute.PUBLISH
import com.exactpro.th2.common.schema.message.storeEvent
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandler
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandlerFactory
import com.exactpro.th2.conn.dirty.tcp.core.api.IManglerFactory
import com.exactpro.th2.conn.dirty.tcp.core.api.impl.DummyManglerFactory.DummyMangler
import com.exactpro.th2.conn.dirty.tcp.core.api.impl.HandlerContext
import com.exactpro.th2.conn.dirty.tcp.core.api.impl.ManglerContext
import com.exactpro.th2.conn.dirty.tcp.core.util.eventId
import com.exactpro.th2.conn.dirty.tcp.core.util.logId
import com.exactpro.th2.conn.dirty.tcp.core.util.messageId
import com.exactpro.th2.conn.dirty.tcp.core.util.sessionAlias
import com.exactpro.th2.conn.dirty.tcp.core.util.toErrorEvent
import com.exactpro.th2.conn.dirty.tcp.core.util.toEvent
import io.netty.channel.nio.NioEventLoopGroup
import mu.KotlinLogging
import java.io.InputStream
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.SECONDS

class Microservice(
    rootEventId: String?,
    private val settings: Settings,
    private val readDictionary: (DictionaryType) -> InputStream,
    private val eventRouter: MessageRouter<EventBatch>,
    private val messageRouter: MessageRouter<MessageGroupBatch>,
    private val handlerFactory: IHandlerFactory,
    private val manglerFactory: IManglerFactory,
    private val registerResource: (name: String, destructor: () -> Unit) -> Unit,
) {
    private val logger = KotlinLogging.logger {}

    private val rootEventId = rootEventId ?: eventRouter.storeEvent("Dirty TCP client".toEvent()).id
    private val errorEventId by lazy { eventRouter.storeEvent("Errors".toErrorEvent(), rootEventId).id }
    private val groupEventIds = ConcurrentHashMap<String, String>()
    private val sessionEventIds = ConcurrentHashMap<String, String>()

    private val executor = Executors.newScheduledThreadPool(settings.appThreads + settings.ioThreads).apply {
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

    private val messageBatcher = MessageBatcher(
        settings.maxBatchSize,
        settings.maxFlushTime,
        if (settings.batchByGroup) GROUP_SELECTOR else ALIAS_SELECTOR,
        executor
    ) { batch ->
        messageRouter.send(batch, QueueAttribute.RAW.value)
        publishSentEvents(batch)
    }.apply {
        registerResource("message-batcher", ::close)
    }

    private val eventBatcher = EventBatcher(settings.maxBatchSize, settings.maxFlushTime, executor) { batch ->
        eventRouter.sendAll(batch, PUBLISH.toString(), EVENT.toString())
    }.apply {
        registerResource("event-batcher", ::close)
    }

    private val handlers = ConcurrentHashMap<String, MutableMap<String, IHandler>>()

    private val channelFactory = ChannelFactory(
        executor,
        eventLoopGroup,
        eventBatcher::onEvent,
        messageBatcher::onMessage,
        { event, parentId -> eventRouter.storeEvent(event, parentId).id },
        settings.publishConnectEvents
    )

    init {
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

        runCatching {
            checkNotNull(messageRouter.subscribe(::handleBatch, INPUT_QUEUE_ATTRIBUTE))
        }.onSuccess { monitor ->
            registerResource("raw-monitor", monitor::unsubscribe)
        }.onFailure {
            throw IllegalStateException("Failed to subscribe to input queue", it)
        }
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
        val sessionGroup = rawMessage.sessionGroup.ifBlank { sessionAlias }

        val handler = channelFactory.getHandler(sessionGroup, sessionAlias) ?: run {
            onError("Unknown session group or alias: $sessionGroup/$sessionAlias", message)
            return
        }

        handler.send(rawMessage).exceptionally {
            onError("Failed to send message", message, it)
            null
        }
    }

    private fun initSession(session: SessionSettings) {
        val sessionGroup = session.sessionGroup
        val sessionAlias = session.sessionAlias

        val groupEventId = groupEventIds.getOrPut(sessionGroup) {
            eventRouter.storeEvent("Group: $sessionGroup".toEvent(), rootEventId).id
        }

        val sessionEventId = sessionEventIds.getOrPut(sessionAlias) {
            eventRouter.storeEvent("Session: $sessionAlias".toEvent(), groupEventId).id
        }

        val sendEvent: (Event) -> Unit = { onEvent(it, sessionEventId) }

        val handlerContext = HandlerContext(session.handler, sessionAlias, channelFactory, readDictionary, sendEvent)
        val handler = handlerFactory.create(handlerContext)

        val mangler = when (val settings = session.mangler) {
            null -> DummyMangler
            else -> manglerFactory.create(ManglerContext(settings, readDictionary, sendEvent))
        }
        
        channelFactory.registerSession(sessionGroup, sessionAlias, handler, mangler, sessionEventId)

        registerResource("handler-$sessionAlias", handler::close)
        registerResource("mangler-$sessionAlias", mangler::close)

        handlers.getOrPut(sessionGroup, ::ConcurrentHashMap)[sessionAlias] = handler
    }

    private fun publishSentEvents(batch: MessageGroupBatch) {
        if (!settings.publishSentEvents) return

        val eventMessages = HashMap<EventID, MutableList<MessageID>>().apply {
            for (group in batch.groupsList) {
                val message = group.messagesList[0].rawMessage
                val eventId = message.eventId ?: continue
                if (message.direction != SECOND) continue
                getOrPut(eventId, ::ArrayList) += message.metadata.id
            }
        }

        for ((eventId, messageIds) in eventMessages) {
            val event = "Sent ${messageIds.size} message(s)".toEvent()
            messageIds.forEach(event::messageID)
            onEvent(event, eventId.id)
        }
    }

    private fun onEvent(event: Event, parentId: String) {
        eventBatcher.onEvent(event.toProto(toEventID(parentId)))
    }

    private fun onError(error: String, message: AnyMessage, cause: Throwable? = null) {
        val id = message.messageId
        val event = error.toErrorEvent(cause).messageID(id)
        logger.error("$error (message: ${id.logId})", cause)
        onEvent(event, message.getErrorEventId())
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
            onEvent(event, parentEventId)
        }
    }

    private fun AnyMessage.getErrorEventId(): String {
        return (eventId?.id ?: sessionEventIds[sessionAlias]) ?: errorEventId
    }

    companion object {
        private const val INPUT_QUEUE_ATTRIBUTE = "send"
    }
}

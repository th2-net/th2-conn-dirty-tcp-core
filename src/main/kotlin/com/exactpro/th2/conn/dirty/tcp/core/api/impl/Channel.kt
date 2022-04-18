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

package com.exactpro.th2.conn.dirty.tcp.core.api.impl

import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.conn.dirty.tcp.core.Pipe.Companion.newPipe
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel.SendMode
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel.SendMode.HANDLE_AND_MANGLE
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolHandler
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolMangler
import com.exactpro.th2.conn.dirty.tcp.core.netty.ITcpChannelHandler
import com.exactpro.th2.conn.dirty.tcp.core.netty.TcpChannel
import com.exactpro.th2.conn.dirty.tcp.core.util.asExpandable
import com.exactpro.th2.conn.dirty.tcp.core.util.attachMessage
import com.exactpro.th2.conn.dirty.tcp.core.util.eventId
import com.exactpro.th2.conn.dirty.tcp.core.util.submitWithRetry
import com.exactpro.th2.conn.dirty.tcp.core.util.toByteBuf
import com.exactpro.th2.conn.dirty.tcp.core.util.toErrorEvent
import com.exactpro.th2.conn.dirty.tcp.core.util.toEvent
import com.exactpro.th2.conn.dirty.tcp.core.util.toMessage
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil.hexDump
import io.netty.channel.EventLoopGroup
import mu.KotlinLogging
import org.jctools.queues.MpscUnboundedArrayQueue
import java.net.InetSocketAddress
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import com.exactpro.th2.common.event.Event as CommonEvent

class Channel(
    private val defaultAddress: InetSocketAddress,
    private val defaultSecure: Boolean,
    private val sessionAlias: String,
    private val reconnectDelay: Long,
    private val handler: IProtocolHandler,
    private val mangler: IProtocolMangler,
    onEvent: (Event) -> Unit,
    onMessage: (RawMessage) -> Unit,
    private val executor: ScheduledExecutorService,
    private val eventLoopGroup: EventLoopGroup,
    val parentEventId: EventID,
) : IChannel, ITcpChannelHandler {
    private val logger = KotlinLogging.logger {}
    private val lock = ReentrantLock()

    private val ioEvents = executor.newPipe("io-events-$sessionAlias", MpscUnboundedArrayQueue(1024), Runnable::run)
    private val messages = executor.newPipe("messages-$sessionAlias", consumer = onMessage)
    private val events = executor.newPipe("events-$sessionAlias", consumer = onEvent)

    @Volatile private var reconnect = true
    @Volatile private var channel = createChannel(defaultAddress, defaultSecure)
    private var connectFuture: Future<Unit> = CompletableFuture.completedFuture(Unit)

    override val address: InetSocketAddress
        get() = channel.address

    override val isOpen: Boolean
        get() = channel.isOpen

    override val isSecure: Boolean
        get() = channel.isSecure

    override fun open() = open(defaultAddress, defaultSecure)

    override fun open(address: InetSocketAddress, secure: Boolean): Unit = openAsync(address, secure).get()

    private fun openAsync(address: InetSocketAddress, secure: Boolean): Future<Unit> {
        logger.debug { "Trying to connect to: $address (session: $sessionAlias)" }

        reconnect = true

        lock.withLock {
            if (isOpen) {
                logger.warn { "Already connected to: ${channel.address} (session: $sessionAlias)" }
                return connectFuture
            }

            if (!connectFuture.isDone) {
                logger.warn { "Already connecting to: ${channel.address} (session: $sessionAlias)" }
                return connectFuture
            }

            if (address != channel.address || secure != channel.isSecure) {
                channel = createChannel(address, secure)
            }

            connectFuture = executor.submitWithRetry(reconnectDelay) {
                if (reconnect) {
                    onInfo("Connecting to: $address (session: $sessionAlias)")

                    runCatching(channel::open).onFailure {
                        onError("Failed to connect to: $address (session: $sessionAlias)", it)
                        throw it
                    }

                    Result.success(Unit)
                } else {
                    Result.failure(IllegalStateException("Stopped connection attempts to: $address (session: $sessionAlias)"))
                }
            }

            return connectFuture
        }
    }

    fun send(message: RawMessage) = sendInternal(
        message = message.body.toByteBuf(),
        metadata = message.metadata.propertiesMap.toMutableMap(),
        parentEventId = message.eventId
    )

    override fun send(message: ByteBuf, metadata: Map<String, String>, mode: SendMode) = sendInternal(
        message = message,
        metadata = metadata.toMutableMap(),
        mode = mode
    )

    private fun sendInternal(
        message: ByteBuf,
        metadata: MutableMap<String, String>,
        mode: SendMode = HANDLE_AND_MANGLE,
        parentEventId: EventID? = null,
    ): MessageID = lock.withLock {
        check(isOpen) { "Cannot send message. Not connected to: $address (session: $sessionAlias)" }

        val buffer = message.asExpandable()

        if (mode.handle) handler.onOutgoing(buffer, metadata)

        val event = if (mode.mangle) mangler.onOutgoing(buffer, metadata) else null
        val protoMessage = buffer.toMessage(sessionAlias, SECOND, metadata, parentEventId)

        channel.send(buffer.asReadOnly())

        if (mode.mangle) mangler.afterOutgoing(buffer, metadata)

        event?.run { storeEvent(attachMessage(protoMessage), parentEventId ?: this@Channel.parentEventId) }
        storeMessage(protoMessage)

        protoMessage.metadata.id
    }

    override fun close() {
        reconnect = false

        lock.withLock {
            if (isOpen) {
                onInfo("Disconnecting from: $address (session: $sessionAlias)")

                runCatching(channel::close).onFailure {
                    onError("Failed to disconnect from: $address (session: $sessionAlias)", it)
                }
            }
        }
    }

    override fun onOpen() {
        onInfo("Connected to: $address (session: $sessionAlias)")
        handler.onOpen()
        mangler.onOpen()
    }

    override fun onReceive(buffer: ByteBuf): ByteBuf? {
        logger.trace { "Received data on '$sessionAlias' session: ${hexDump(buffer)}" }
        return handler.onReceive(buffer)
    }

    override fun onMessage(message: ByteBuf) {
        logger.trace { "Received message on '$sessionAlias' session: ${hexDump(message)}" }
        val metadata = handler.onIncoming(message.asReadOnly())
        mangler.onIncoming(message.asReadOnly(), metadata)
        val protoMessage = message.toMessage(sessionAlias, FIRST, metadata)
        storeMessage(protoMessage)
        message.release()
    }

    override fun onError(cause: Throwable) = onError("Error on: $address (session: $sessionAlias)", cause)

    override fun onClose() {
        onInfo("Disconnected from: $address (session: $sessionAlias)")

        runCatching(handler::onClose).onFailure(::onError)
        runCatching(mangler::onClose).onFailure(::onError)

        if (reconnect) {
            executor.schedule({ if (!isOpen && reconnect) openAsync(defaultAddress, defaultSecure) }, reconnectDelay, MILLISECONDS)
        }
    }

    private fun onInfo(message: String) {
        logger.info(message)
        storeEvent(message.toEvent(), parentEventId)
    }

    private fun onError(message: String, cause: Throwable) {
        logger.error(message, cause)
        storeEvent(message.toErrorEvent(cause), parentEventId)
    }

    private fun storeMessage(message: RawMessage) {
        if (!messages.send(message)) {
            logger.error { "Failed to store message from '$sessionAlias' session due to overflow: ${message.toJson()}" }
        }
    }

    private fun storeEvent(event: CommonEvent, parentEventId: EventID) = storeEvent(event.toProto(parentEventId))

    private fun storeEvent(event: Event) {
        if (!events.send(event)) {
            logger.error { "Failed to store event from '$sessionAlias' session due to overflow: ${event.toJson()}" }
        }
    }

    private fun createChannel(address: InetSocketAddress, secure: Boolean) = TcpChannel(
        address,
        secure,
        eventLoopGroup,
        ioEvents::send,
        this
    )
}

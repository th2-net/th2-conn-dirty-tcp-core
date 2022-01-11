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

package com.exactpro.th2.conn.dirty.tcp.core.api.impl

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.conn.dirty.tcp.core.TaskSequencePool
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
import com.exactpro.th2.conn.dirty.tcp.core.util.toByteBuf
import com.exactpro.th2.conn.dirty.tcp.core.util.toErrorEvent
import com.exactpro.th2.conn.dirty.tcp.core.util.toEvent
import com.exactpro.th2.conn.dirty.tcp.core.util.toMessage
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil.hexDump
import io.netty.channel.EventLoopGroup
import mu.KotlinLogging
import java.net.InetSocketAddress
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write


class Channel(
    private val defaultAddress: InetSocketAddress,
    private val defaultSecure: Boolean,
    private val sessionAlias: String,
    private val reconnectDelay: Long,
    private val handler: IProtocolHandler,
    private val mangler: IProtocolMangler,
    private val onEvent: (event: Event, parentEventId: EventID) -> Unit,
    private val onMessage: (RawMessage) -> Unit,
    private val eventLoopGroup: EventLoopGroup,
    sequencePool: TaskSequencePool,
    val parentEventId: EventID
) : IChannel, ITcpChannelHandler {
    private val logger = KotlinLogging.logger {}
    private val lock = ReentrantReadWriteLock()
    private val channelSequence = sequencePool.create("channel-events-$sessionAlias", Int.MAX_VALUE)
    private val messageSequence = sequencePool.create("messages-$sessionAlias")
    private val eventSequence = sequencePool.create("events-$sessionAlias")
    @Volatile private var reconnect = true
    @Volatile private var channel = createChannel(defaultAddress, defaultSecure)

    override val address: InetSocketAddress
        get() = channel.address

    override val isOpen: Boolean
        get() = channel.isOpen

    override val isSecure: Boolean
        get() = channel.isSecure

    override fun open() = open(defaultAddress, defaultSecure)

    override fun open(address: InetSocketAddress, secure: Boolean) {
        reconnect = true

        lock.write {
            check(!isOpen) { "Already connected to: ${channel.address}" }

            if (address != channel.address || secure != channel.isSecure) {
                channel = createChannel(address, secure)
            }

            while (!isOpen && reconnect) {
                onInfo("Connecting to: $address")

                runCatching(channel::open).onFailure {
                    onError("Failed to connect to: $address", it)
                    Thread.sleep(reconnectDelay)
                }
            }
        }
    }

    fun send(message: RawMessage) = sendInternal(
        message = message.body.toByteBuf(),
        metadata = message.metadata.propertiesMap,
        parentEventId = message.eventId
    )

    override fun send(message: ByteBuf, metadata: Map<String, String>, mode: SendMode) = sendInternal(
        message = message,
        metadata = metadata,
        mode = mode
    )

    private fun sendInternal(
        message: ByteBuf,
        metadata: Map<String, String> = mapOf(),
        mode: SendMode = HANDLE_AND_MANGLE,
        parentEventId: EventID? = null
    ): MessageID = lock.read {
        if (!isOpen) open()

        val buffer = message.asExpandable()
        val metadata = if (mode.handle) handler.onOutgoing(buffer, metadata) else metadata
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

        lock.write {
            if (isOpen) {
                onInfo("Disconnecting from: $address")

                runCatching(channel::close).onFailure {
                    onError("Failed to disconnect from: $address", it)
                }
            }
        }
    }

    override fun onOpen() {
        onInfo("Connected to: $address")
        handler.onOpen()
        mangler.onOpen()
    }

    override fun onReceive(buffer: ByteBuf): ByteBuf? {
        logger.trace { "Received data: ${hexDump(buffer)}" }
        return handler.onReceive(buffer)
    }

    override fun onMessage(message: ByteBuf) {
        logger.trace { "Received message: ${hexDump(message)}" }
        val metadata = handler.onIncoming(message.asReadOnly())
        mangler.onIncoming(message.asReadOnly(), metadata)
        val protoMessage = message.toMessage(sessionAlias, FIRST, metadata)
        storeMessage(protoMessage)
    }

    override fun onError(cause: Throwable) = onError("Error on: $address", cause)

    override fun onClose() {
        onInfo("Disconnected from: $address")

        runCatching(handler::onClose).onFailure(::onError)
        runCatching(mangler::onClose).onFailure(::onError)

        if (reconnect) {
            Thread.sleep(reconnectDelay)
            open(channel.address, channel.isSecure)
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

    private fun storeMessage(message: RawMessage) = messageSequence.execute { onMessage(message) }

    private fun storeEvent(event: Event, parentEventId: EventID) = eventSequence.execute { onEvent(event, parentEventId) }

    private fun createChannel(address: InetSocketAddress, secure: Boolean) = TcpChannel(
        address,
        secure,
        eventLoopGroup,
        channelSequence::execute,
        this
    )
}
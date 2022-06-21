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

package com.exactpro.th2.conn.dirty.tcp.core.api

import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel.SendMode.HANDLE_AND_MANGLE
import io.netty.buffer.ByteBuf
import java.net.InetSocketAddress
import javax.annotation.concurrent.ThreadSafe

/**
 * Represents a single TCP connection
 */
@ThreadSafe
interface IChannel {
    /**
     * Returns current channel address
     */
    val address: InetSocketAddress

    /**
     * Returns `true` if this channel is open
     */
    val isOpen: Boolean

    /**
     * Returns `true` if this channel is using encryption
     */
    val isSecure: Boolean

    val sessionAlias: String

    /**
     * Opens this channel using default address (i.e. establishes a TCP connection).
     *
     * It will fail if channel is already open.
     *
     * If operation was successful [IProtocolHandler.onOpen] and [IProtocolMangler.onOpen] methods will be called next
     */
    fun open()


    /**
     * Opens this channel using the specified [address] (i.e. establishes a TCP connection).
     *
     * It will fail if channel is already open.
     *
     * If operation was successful [IProtocolHandler.onOpen] and [IProtocolMangler.onOpen] methods will be called next
     *
     * @param address address to open channel for
     * @param secure `true` if channel must use encryption
     */
    fun open(address: InetSocketAddress, secure: Boolean = false)

    /**
     * Sends [message] to this channel (if channel is closed it will be opened first).
     *
     * Depending on send [mode] message and [metadata] could be passed to [IProtocolHandler.onOutgoing] and/or [IProtocolMangler.onOutgoing]
     * methods which can modify message content and substitute metadata (in case of [IProtocolHandler.onOutgoing]).
     *
     * If mode is set to [SendMode.HANDLE_AND_MANGLE] or [SendMode.MANGLE] message and metadata will be passed to [IProtocolMangler.afterOutgoing] after send
     *
     * For example, in case if mode is set to [SendMode.HANDLE_AND_MANGLE] processing sequence will look like this:
     *
     * ```
     * val metadata = handler.onOutgoing(message, metadata)
     * val event = mangler.onOutgoing(message, metadata)
     *
     * socket.send(message)
     * mangler.afterOutgoing(message, metadata)
     * ```
     *
     * @param message message content (can be passed to handler and/or mangler)
     * @param metadata message metadata (can be passed to handler and/or mangler)
     * @param mode message send mode (specifies who would handle message/metadata - handler, mangler, both or none of them)
     *
     * @return ID of sent message
     */
    fun send(message: ByteBuf, metadata: Map<String, String> = mapOf(), mode: SendMode = HANDLE_AND_MANGLE): MessageID

    /**
     * Closes this channel (i.e. closes a TCP connection) gracefully.
     *
     * Unlike unexpected channel closure (i.e. caused by remote host or inbound message handling error) it won't trigger reconnection.
     *
     * If operation was successful [IProtocolHandler.onClose] and [IProtocolMangler.onClose] methods will be called next.
     *
     * If channel is already closed it will have no effect
     */
    fun close()

    enum class SendMode(val handle: Boolean, val mangle: Boolean) {
        /**
         * Message and its metadata will pass through [IProtocolHandler.onOutgoing] and [IProtocolMangler.onOutgoing] before send
         */
        HANDLE_AND_MANGLE(true, true),

        /**
         * Message and its metadata will only be passed to [IProtocolHandler.onOutgoing] before send
         */
        HANDLE(true, false),

        /**
         * Message and its metadata will only be passed to [IProtocolMangler.onOutgoing] before send
         */
        MANGLE(false, true),

        /**
         * Message will be sent directly to socket
         */
        DIRECT(false, false)
    }
}
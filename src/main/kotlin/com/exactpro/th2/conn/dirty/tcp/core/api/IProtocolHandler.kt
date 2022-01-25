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

import io.netty.buffer.ByteBuf
import javax.annotation.concurrent.ThreadSafe

/**
 * Handles protocol messages and events, maintains session on a single [channel][IChannel]
 */
@ThreadSafe
interface IProtocolHandler : AutoCloseable {
    /**
     * This method is called after a corresponding channel has been opened (e.g. TCP connection is established).
     *
     * For example, it can be used to perform protocol startup routine (e.g. send a logon)
     */
    fun onOpen() = Unit

    /**
     * This method is called when data is received through a corresponding channel.
     *
     * Purpose of this method is to read a single message from this [buffer] (if any) and return it as a separate buffer.
     *
     * This method will be called on the buffer as long as it doesn't return `null`.
     *
     * If a message was read from the buffer [ByteBuf.readerIndex] must be increased by the size of the read message.
     *
     * This method must be stateless and as lightweight as possible since it's the only handler method which will be called from IO-thread
     *
     * @param buffer buffer containing data received by a channel
     *
     * @return buffer with a single protocol message or `null` if the buffer doesn't contain a complete message
     */
    fun onReceive(buffer: ByteBuf): ByteBuf?

    /**
     * This method is called for each [message] read from a corresponding channel.
     *
     * It should analyze the message and return corresponding metadata.
     *
     * It can be used to change state according to received message (e.g. set state to logged-in when a login response is received).
     *
     * @param message received message
     *
     * @return message metadata
     */
    fun onIncoming(message: ByteBuf): Map<String, String> = mapOf()

    /**
     * This method is can be called before sending [message] to a corresponding channel (whether it'll be called or not depends on [send-mode][IChannel.SendMode]).
     *
     * It should analyze message and its metadata and modify message (e.g. add header) if required.
     *
     * It also returns message metadata which can passed to [IProtocolMangler.onOutgoing] and will be stored in [RawMessage][com.exactpro.th2.common.grpc.RawMessage]
     *
     * It can also be used to change state according to outgoing message (e.g. schedule a re-login when logout message is being sent).
     *
     * @param message mutable buffer with outgoing message
     * @param metadata message metadata
     * @return new message metadata
     */
    fun onOutgoing(message: ByteBuf, metadata: Map<String, String>): Map<String, String> = metadata

    /**
     * This method is called after a corresponding channel has been closed (e.g. TCP connection is closed).
     *
     * For example, it can be used to perform cleanup session-related resources and schedule reconnect
     */
    fun onClose() = Unit

    /**
     * This method is called when microservice shutdown process was initiated
     *
     * For example, it can be used to clean-up long-living resources (executors, schedulers, etc.)
     */
    override fun close() = Unit
}


interface IProtocolHandlerSettings

interface IProtocolHandlerFactory : IFactory<IProtocolHandler, IProtocolHandlerSettings>
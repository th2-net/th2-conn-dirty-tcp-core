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

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.MessageID
import io.netty.buffer.ByteBuf
import javax.annotation.concurrent.ThreadSafe

/**
 * Mangles protocol messages on a set of [channels][IChannel] belonging to a single session
 */
@ThreadSafe
interface IMangler : AutoCloseable {
    /**
     * This method is called after a corresponding [channel] has been opened (e.g. TCP connection is established).
     */
    fun onOpen(channel: IChannel): Unit = Unit

    /**
     * This method is called for each [message] read from a corresponding [channel].
     *
     * It can be used to change mangling algorithm after a certain message was received
     *
     * @param message received message
     * @param messageID messageId of the received message after it will be saved to mstore
     *
     * @return message metadata
     */
    fun onIncoming(channel: IChannel, message: ByteBuf, metadata: Map<String, String>, messageID: MessageID): Unit = Unit

    /**
     * This method is called before sending [message] to a corresponding [channel] (whether it'll be called or not depends on [send-mode][IChannel.SendMode]).
     *
     * It should analyze message and its metadata and modify them (e.g. add header or a metadata property) if required.
     *
     * If message was modified it must return event with modifications description
     *
     * It can also be used to send a message before this one or even close the corresponding channel
     *
     * @param message mutable buffer with outgoing message
     * @param metadata message metadata
     * @return event with message modifications descriptions or `null` if there wasn't any
     */
    fun onOutgoing(channel: IChannel, message: ByteBuf, metadata: MutableMap<String, String>): Event?

    /**
     * This method is called after [message] was sent to a corresponding [channel] (whether it'll be called or not depends on [send-mode][IChannel.SendMode])
     *
     * For example, it can be used close a corresponding channel after a certain message was sent
     *
     * @param message buffer with sent message
     * @param metadata message metadata
     */
    fun postOutgoing(channel: IChannel, message: ByteBuf, metadata: Map<String, String>): Unit = Unit

    /**
     * This method is called after a corresponding [channel] has been closed (e.g. TCP connection is closed).
     *
     * For example, it can be used to perform cleanup session-related resources and schedule reconnect
     */
    fun onClose(channel: IChannel): Unit = Unit

    /**
     * This method is called when microservice shutdown process was initiated
     *
     * For example, it can be used to clean-up long-living resources (executors, schedulers, etc.)
     */
    override fun close(): Unit = Unit
}

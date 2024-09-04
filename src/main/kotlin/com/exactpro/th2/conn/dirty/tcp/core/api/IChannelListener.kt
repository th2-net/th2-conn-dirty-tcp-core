/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import io.netty.buffer.ByteBuf
import javax.annotation.concurrent.ThreadSafe

internal val EMPTY_LISTENER = object : IChannelListener {}

/**
 * Listens [IChannel] events related to performed operations.
 * Implementation shouldn't modify state of passed arguments.
 * This interface can be implemented the same class implements [IHandler]
 */
@ThreadSafe
@JvmDefaultWithoutCompatibility
interface IChannelListener {
    /**
     * This method is called after outgoing [message] was published to MQ (whether it'll be called or not depends on [send-mode][IChannel.SendMode])
     *
     * @param message buffer with published message
     * @param metadata message metadata
     */
    fun postOutgoingMqPublish(
        channel: IChannel,
        message: ByteBuf,
        messageId: MessageID,
        metadata: MutableMap<String, String>,
        eventId: EventID?
    ): Unit = Unit
}
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

package com.exactpro.th2.conn.dirty.tcp.core.api.impl

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.schema.dictionary.DictionaryType
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter
import com.exactpro.th2.conn.dirty.tcp.core.ChannelFactory
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel.Security
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandlerContext
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandlerSettings
import java.io.InputStream
import java.net.InetSocketAddress

class HandlerContext(
    override val settings: IHandlerSettings,
    private val sessionAlias: String,
    private val channelFactory: ChannelFactory,
    private val getDictionary: (DictionaryType) -> InputStream,
    private val sendEvent: (Event) -> Unit,
    private val getService: (Class<out Any>) -> Any
) : IHandlerContext {
    override fun createChannel(
        address: InetSocketAddress,
        security: Security,
        attributes: Map<String, Any>,
        autoReconnect: Boolean,
        reconnectDelay: Long,
        maxMessageRate: Int,
        vararg sessionSuffixes: String,
    ): IChannel = channelFactory.createChannel(
        address,
        security,
        attributes,
        autoReconnect,
        reconnectDelay,
        maxMessageRate,
        sessionAlias,
        *sessionSuffixes,
    )

    override fun destroyChannel(channel: IChannel): Unit = channelFactory.destroyChannel(channel)
    override fun get(dictionary: DictionaryType): InputStream = getDictionary(dictionary)
    override fun send(event: Event): Unit = sendEvent(event)
    override fun <T : Any> getGrpcService(serviceClass: Class<T>): T = getService(serviceClass) as T
}
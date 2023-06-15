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

package com.exactpro.th2.conn.dirty.tcp.core.api

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.schema.dictionary.DictionaryType
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel.Security
import java.io.InputStream
import java.net.InetSocketAddress
import javax.annotation.concurrent.ThreadSafe

/**
 * Single handler context
 */
@ThreadSafe
interface IHandlerContext {
    /**
     * Returns settings of a [handler][IHandler]
     */
    val settings: IHandlerSettings

    /**
     * Returns book name for this service
     */
    val bookName: String

    /**
     * Creates channel for specified [address] with specified [security], [attributes] and [sessionSuffixes]
     */
    fun createChannel(
        address: InetSocketAddress,
        security: Security,
        attributes: Map<String, Any> = mapOf(),
        autoReconnect: Boolean = true,
        reconnectDelay: Long = 5000,
        maxMessageRate: Int = Int.MAX_VALUE,
        vararg sessionSuffixes: String,
    ): IChannel

    /**
     * Destroys provided [channel]
     */
    fun destroyChannel(channel: IChannel)

    /**
     * Returns input stream with requested [dictionary]
     *
     * @param dictionary dictionary type
     *
     * @return dictionary input stream
     */
    operator fun get(dictionary: DictionaryType): InputStream

    /**
     * Returns input stream with requested [dictionary]
     *
     * @param dictionary dictionary alias
     *
     * @return dictionary input stream
     */
    operator fun get(dictionary: String): InputStream

    /**
     * Sends an [event]
     *
     * @param event event to send
     */
    fun send(event: Event)

    /**
     * Returns instance of class to interact with grpc service.
     */
    fun <T: Any> getGrpcService(serviceClass: Class<T>): T
}
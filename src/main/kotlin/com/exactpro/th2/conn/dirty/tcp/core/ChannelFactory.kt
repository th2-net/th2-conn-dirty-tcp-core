/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel.Security
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandler
import com.exactpro.th2.conn.dirty.tcp.core.api.IMangler
import com.exactpro.th2.conn.dirty.tcp.core.api.impl.Channel
import com.exactpro.th2.conn.dirty.tcp.core.util.toEvent
import io.netty.buffer.ByteBuf
import io.netty.channel.EventLoopGroup
import io.netty.handler.traffic.GlobalTrafficShapingHandler
import java.lang.String.join
import java.net.InetSocketAddress
import java.util.concurrent.ScheduledExecutorService
import com.exactpro.th2.common.event.Event as CommonEvent

class ChannelFactory(
    private val executor: ScheduledExecutorService,
    private val eventLoopGroup: EventLoopGroup,
    private val shaper: GlobalTrafficShapingHandler,
    private val onEvent: (Event) -> Unit,
    private val onMessage: MessageAcceptor,
    private val createEvent: (event: CommonEvent, parentId: EventID) -> EventID,
    private val publishConnectEvents: Boolean,
) {
    private val sessions = HashMap<String, SessionContext>()
    private val channels = HashMap<String, IChannel>()

    fun registerSession(
        sessionGroup: String,
        sessionAlias: String,
        handler: IHandler,
        mangler: IMangler,
        eventId: EventID,
    ): Unit = synchronized(this) {
        require(sessionAlias !in sessions) { "Session is already registered: $sessionAlias" }
        sessions[sessionAlias] = SessionContext(sessionGroup, handler, mangler, eventId, true)
    }

    fun createChannel(
        address: InetSocketAddress,
        security: Security,
        attributes: Map<String, Any> = mapOf(),
        autoReconnect: Boolean,
        reconnectDelay: Long,
        maxMessageRate: Int,
        sessionAlias: String,
        vararg sessionSuffixes: String,
    ): IChannel = synchronized(this) {
        val context = sessions[sessionAlias] ?: error("Session does not exist: $sessionAlias")
        require(context.isRoot) { "Parent session is a non-root one: $sessionAlias" }

        val genSessionAlias = join("_", sessionAlias, *sessionSuffixes)
        require(genSessionAlias !in channels) { "Session channel already exists: $genSessionAlias" }

        val channel = Channel(
            address,
            security,
            attributes,
            context.group,
            sessionAlias,
            autoReconnect,
            reconnectDelay,
            maxMessageRate,
            publishConnectEvents,
            context.handler,
            context.mangler,
            onEvent,
            onMessage,
            executor,
            eventLoopGroup,
            shaper,
            createEvent("Channel: $sessionAlias".toEvent(), context.eventId)
        )

        channels[genSessionAlias] = channel

        if (sessionSuffixes.isNotEmpty()) {
            sessions[genSessionAlias] = context.copy(isRoot = false)
        }

        return channel
    }

    fun destroyChannel(channel: IChannel): Unit = synchronized(this) {
        val sessionAlias = channel.sessionAlias
        val context = sessions[sessionAlias] ?: error("Unknown session: $sessionAlias")
        channel.close()
        channels -= sessionAlias
        if (!context.isRoot) sessions -= sessionAlias
    }

    fun getHandler(sessionGroup: String, sessionAlias: String): IHandler? = synchronized(this) {
        return sessions[sessionAlias]?.takeIf { it.group == sessionGroup }?.handler
    }

    fun interface MessageAcceptor {
        fun accept(buff: ByteBuf, messageId: MessageID, metadata: Map<String, String>, eventId: EventID?)
    }

    private data class SessionContext(
        val group: String,
        val handler: IHandler,
        val mangler: IMangler,
        val eventId: EventID,
        val isRoot: Boolean,
    )
}
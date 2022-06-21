package com.exactpro.th2.conn.dirty.tcp.core.api

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.conn.dirty.tcp.core.SessionSettings
import com.exactpro.th2.conn.dirty.tcp.core.TaskSequencePool
import io.netty.channel.nio.NioEventLoop
import io.netty.channel.nio.NioEventLoopGroup
import java.net.InetSocketAddress
import java.util.concurrent.ScheduledExecutorService

interface IChannelFactory {
    val eventRouter: MessageRouter<EventBatch>
    val taskSequencePool: TaskSequencePool
    val registerResource: (name: String, destructor: () -> Unit) -> Unit
    val sessionSettings: SessionSettings
    val reconnectDelay: Long
    val rootEventId: String
    val handler: IProtocolHandler
    val mangler: IProtocolMangler
    val onEvent: (event: Event, parentEventId: EventID) -> Unit
    val onMessage: (message: RawMessage) -> Unit
    val executor: ScheduledExecutorService
    val nioEventLoop: NioEventLoopGroup

    fun create(address: InetSocketAddress, isSecure: Boolean, reconnectDelay: Long = 0L): IChannel
    fun create(): IChannel
}
package com.exactpro.th2.conn.dirty.tcp.core.api.impl

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.storeEvent
import com.exactpro.th2.conn.dirty.tcp.core.SessionSettings
import com.exactpro.th2.conn.dirty.tcp.core.TaskSequencePool
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannelFactory
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolHandler
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolMangler
import com.exactpro.th2.conn.dirty.tcp.core.util.toEvent
import io.netty.channel.nio.NioEventLoopGroup
import java.net.InetSocketAddress
import java.time.LocalDateTime
import java.util.concurrent.ScheduledExecutorService

class ChannelFactory(
    override val eventRouter: MessageRouter<EventBatch>,
    override val taskSequencePool: TaskSequencePool,
    override val registerResource: (name: String, destructor: () -> Unit) -> Unit,
    override val rootEventId: String,
    override val handler: IProtocolHandler,
    override val mangler: IProtocolMangler,
    override val onEvent: (event: Event, parentEventId: EventID) -> Unit,
    override val onMessage: (message: RawMessage) -> Unit,
    override val executor: ScheduledExecutorService,
    override val nioEventLoop: NioEventLoopGroup,
    override val sessionSettings: SessionSettings,
    override val reconnectDelay: Long,
) : IChannelFactory {

    override fun create(
        address: InetSocketAddress,
        isSecure: Boolean,
        reconnectDelay: Long
    ): IChannel {
        val sessionAlias = "${sessionSettings.sessionAlias}-${LocalDateTime.now()}"
        val parentEventId = eventRouter.storeEvent(sessionAlias.toEvent(), rootEventId).id

        val channel = Channel(
            address,
            isSecure,
            sessionAlias,
            reconnectDelay,
            handler,
            mangler,
            onEvent,
            onMessage,
            executor,
            nioEventLoop,
            taskSequencePool,
            EventID.newBuilder().setId(parentEventId).build()
        )

        registerResource("channel-$sessionAlias", channel::close)
        registerResource("handler-$sessionAlias", handler::close)
        registerResource("mangler-$sessionAlias", mangler::close)

        taskSequencePool.create("send-$sessionAlias")
        return channel
    }

    override fun create(): IChannel {
        return create(
            InetSocketAddress(sessionSettings.host, sessionSettings.port),
            sessionSettings.secure,
            reconnectDelay
        )
    }
}
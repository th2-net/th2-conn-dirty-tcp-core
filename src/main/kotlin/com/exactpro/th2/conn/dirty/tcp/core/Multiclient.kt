package com.exactpro.th2.conn.dirty.tcp.core

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolHandler
import com.exactpro.th2.conn.dirty.tcp.core.util.toByteBuf

class Multiclient(
    private val handler: IProtocolHandler,
    private val sequencePool: TaskSequencePool,
    private val onError: (error: String, message: AnyMessage, cause: Throwable?) -> Unit
) {
    fun send(rawMessage: RawMessage, message: AnyMessage) {
        val channel = handler.getChannel(rawMessage.body.toByteBuf(), rawMessage.metadata.propertiesMap)
        sequencePool["send-${channel.sessionAlias}"].execute {
            rawMessage.runCatching(channel::send).recoverCatching { cause ->
                onError("Failed to send message", message, cause)
            }
        }
    }
}
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

@file:JvmName("CommonUtil")

package com.exactpro.th2.conn.dirty.tcp.core.util

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.Event.Status.PASSED
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.AnyMessage.KindCase.MESSAGE
import com.exactpro.th2.common.grpc.AnyMessage.KindCase.RAW_MESSAGE
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.grpc.Direction.UNRECOGNIZED
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.direction
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.message.toTimestamp
import com.google.protobuf.ByteString
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import org.apache.commons.lang3.exception.ExceptionUtils
import java.time.Instant
import java.util.ServiceLoader
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicLong

private val INCOMING_SEQUENCES = ConcurrentHashMap<String, () -> Long>()
private val OUTGOING_SEQUENCES = ConcurrentHashMap<String, () -> Long>()

private fun getSequenceGenerator(): () -> Long = Instant.now().run {
    AtomicLong(epochSecond * SECONDS.toNanos(1) + nano)
}::incrementAndGet

private fun String.getSequence(direction: Direction) = when (direction) {
    FIRST -> INCOMING_SEQUENCES.getOrPut(this, ::getSequenceGenerator)
    SECOND -> OUTGOING_SEQUENCES.getOrPut(this, ::getSequenceGenerator)
    UNRECOGNIZED -> error("Unknown direction $direction in session: $this")
}.invoke()

fun ByteBuf.toMessage(
    sessionAlias: String,
    direction: Direction,
    metadata: Map<String, String>,
    parentEventId: EventID? = null
): RawMessage = RawMessage.newBuilder().run {
    parentEventId?.let { this.parentEventId = it }

    this.body = ByteString.copyFrom(asReadOnly().nioBuffer())
    this.sessionAlias = sessionAlias
    this.direction = direction
    this.sequence = sessionAlias.getSequence(direction)

    this.metadataBuilder.apply {
        putAllProperties(metadata)
        timestamp = Instant.now().toTimestamp()
    }

    build()
}

fun String.toEvent(): Event = toEvent(PASSED)

fun String.toErrorEvent(cause: Throwable? = null): Event = toEvent(FAILED, cause)

private fun String.toEvent(
    status: Status,
    cause: Throwable? = null
) = Event.start().apply {
    name(this@toEvent)
    type(if (status == PASSED) "Info" else "Error")
    status(status)
    generateSequence(cause, Throwable::cause)
        .map(ExceptionUtils::getMessage)
        .map(EventUtils::createMessageBean)
        .forEach(::bodyData)
}


fun Event.attachMessage(message: RawMessage): Event = messageID(message.metadata.id)

fun RawMessage.toGroup(): MessageGroup = MessageGroup.newBuilder().run {
    plusAssign(this@toGroup)
    build()
}

fun ByteString.toByteBuf(): ByteBuf = asReadOnlyByteBuffer().run(Unpooled.buffer(size())::writeBytes)

val MessageID.logId: String
    get() = buildString {
        append(connectionId.sessionAlias)
        append(":")
        append(direction.toString().lowercase())
        append(":")
        append(sequence)
        subsequenceList.joinTo(this, "") { ".$it" }
    }

val RawMessage.eventId: EventID?
    get() = if (hasParentEventId()) parentEventId else null

val AnyMessage.eventId: EventID?
    get() = when (kindCase) {
        RAW_MESSAGE -> rawMessage.eventId
        MESSAGE -> message.takeIf(Message::hasParentEventId)?.parentEventId
        else -> error("Cannot get parent event id from $kindCase message: ${toJson()}")
    }

val AnyMessage.messageId: MessageID
    get() = when (kindCase) {
        RAW_MESSAGE -> rawMessage.metadata.id
        MESSAGE -> message.metadata.id
        else -> error("Cannot get message id from $kindCase message: ${toJson()}")
    }

val AnyMessage.sessionAlias: String
    get() = when (kindCase) {
        RAW_MESSAGE -> rawMessage.sessionAlias
        MESSAGE -> message.sessionAlias
        else -> error("Cannot get session alias from $kindCase message: ${toJson()}")
    }

inline fun <reified T> load(): T = ServiceLoader.load(T::class.java).toList().run {
    when {
        isEmpty() -> if (null is T) null as T else error("No instances of ${T::class.simpleName}")
        size == 1 -> this[0]
        else -> error("More than 1 instance of ${T::class.simpleName} has been found: $this")
    }
}
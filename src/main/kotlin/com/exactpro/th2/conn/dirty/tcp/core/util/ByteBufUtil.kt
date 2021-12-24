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

@file:JvmName("ByteBufUtil")

package com.exactpro.th2.conn.dirty.tcp.core.util

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.buffer.search.AbstractSearchProcessorFactory.newKmpSearchProcessorFactory
import java.nio.charset.Charset
import kotlin.text.Charsets.UTF_8

fun ByteBuf.asExpandable(): ByteBuf = when (maxCapacity()) {
    Int.MAX_VALUE -> this
    else -> Unpooled.wrappedBuffer(this, Unpooled.buffer())
}

fun ByteBuf.requireReadable(fromIndex: Int, toIndex: Int) {
    require(fromIndex < toIndex) {
        "fromIndex must be less than toIndex: $fromIndex..$toIndex"
    }

    require(fromIndex >= readerIndex() && toIndex <= writerIndex()) {
        "Range is outside of readable bytes: $fromIndex..$toIndex"
    }
}

fun ByteBuf.requireReadable(index: Int) {
    require(index in readerIndex() until writerIndex()) {
        "Index is outside of readable bytes: $index"
    }
}

@JvmOverloads
fun ByteBuf.indexOf(
    value: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): Int {
    requireReadable(fromIndex, toIndex)
    val valueLength = value.size
    val regionLength = toIndex - fromIndex
    if (regionLength < valueLength) return -1
    val factory = newKmpSearchProcessorFactory(value)
    val indexOf = forEachByte(fromIndex, regionLength, factory.newSearchProcessor())
    return indexOf - valueLength + 1
}

@JvmOverloads
fun ByteBuf.indexOf(
    value: String,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    charset: Charset = UTF_8,
): Int = indexOf(value.toByteArray(charset), fromIndex, toIndex)

@JvmOverloads
fun ByteBuf.contains(
    value: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): Boolean = indexOf(value) >= 0

@JvmOverloads
fun ByteBuf.contains(
    value: String,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    charset: Charset = UTF_8
): Boolean = contains(value.toByteArray(charset), fromIndex, toIndex)

private fun ByteBuf.shift(fromIndex: Int, shiftSize: Int) = apply {
    requireReadable(fromIndex)
    require(shiftSize <= maxWritableBytes()) { "Not enough free space to shift $shiftSize bytes: ${maxWritableBytes()}" }

    val readerIndex = readerIndex()
    val writerIndex = writerIndex()

    if (fromIndex == readerIndex && readerIndex >= shiftSize) {
        readerIndex(readerIndex - shiftSize)
        return@apply
    }

    ensureWritable(shiftSize)
    setBytes(fromIndex + shiftSize, copy(fromIndex, writerIndex - fromIndex))
    writerIndex(writerIndex + shiftSize)
}

fun ByteBuf.insert(value: ByteArray, atIndex: Int): ByteBuf = apply {
    val valueSize = value.size

    if (atIndex == writerIndex()) {
        require(valueSize <= maxWritableBytes()) { "Not enough free space to insert $valueSize bytes: ${maxWritableBytes()}" }
        writeBytes(value)
    } else {
        shift(atIndex, valueSize)
        setBytes(atIndex, value)
    }
}

@JvmOverloads
fun ByteBuf.insert(
    value: String,
    atIndex: Int,
    charset: Charset = UTF_8
): ByteBuf = insert(value.toByteArray(charset), atIndex)

fun ByteBuf.remove(fromIndex: Int, toIndex: Int): ByteBuf = apply {
    requireReadable(fromIndex, toIndex)

    when {
        fromIndex == readerIndex() -> readerIndex(toIndex)
        toIndex == writerIndex() -> writerIndex(fromIndex)
        else -> {
            setBytes(fromIndex, slice(toIndex, writerIndex() - toIndex))
            writerIndex(writerIndex() - (toIndex - fromIndex))
        }
    }
}

@JvmOverloads
fun ByteBuf.remove(
    value: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): Boolean {
    requireReadable(fromIndex, toIndex)
    val atIndex = indexOf(value, fromIndex, toIndex)
    if (atIndex < 0) return false
    remove(atIndex, atIndex + value.size)
    return true
}

@JvmOverloads
fun ByteBuf.remove(
    value: String,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    charset: Charset = UTF_8
): Boolean = remove(value.toByteArray(charset), fromIndex, toIndex)

@JvmOverloads
fun ByteBuf.removeAll(
    value: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): ByteBuf = apply {
    @Suppress("ControlFlowWithEmptyBody")
    while (remove(value, fromIndex, toIndex));
}

@JvmOverloads
fun ByteBuf.removeAll(
    value: String,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    charset: Charset = UTF_8
): ByteBuf = apply {
    @Suppress("ControlFlowWithEmptyBody")
    while (remove(value, fromIndex.coerceAtLeast(readerIndex()), toIndex.coerceAtMost(writerIndex())));
}

@JvmOverloads
fun ByteBuf.replace(
    source: ByteArray,
    target: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): Boolean {
    val sizeDiff = source.size - target.size
    val sourceIndex = indexOf(source, fromIndex, toIndex)

    when {
        sourceIndex < 0 -> return false
        sizeDiff < 0 -> shift(sourceIndex, -sizeDiff)
        sizeDiff > 0 -> remove(sourceIndex, sourceIndex + sizeDiff)
    }

    setBytes(sourceIndex, target)

    return true
}

@JvmOverloads
fun ByteBuf.replace(
    source: String,
    target: String,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    charset: Charset = UTF_8
): Boolean = replace(
    source.toByteArray(charset),
    target.toByteArray(charset),
    fromIndex,
    toIndex
)
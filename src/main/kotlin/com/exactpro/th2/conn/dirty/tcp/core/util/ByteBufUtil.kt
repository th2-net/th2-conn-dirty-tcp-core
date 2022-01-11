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

private fun ByteBuf.requireReadable(fromIndex: Int, toIndex: Int) {
    require(fromIndex < toIndex) {
        "fromIndex must be less than toIndex: $fromIndex..$toIndex"
    }

    require(fromIndex >= readerIndex() && toIndex <= writerIndex()) {
        "Range is outside of readable bytes: $fromIndex..$toIndex"
    }
}

private fun ByteBuf.requireReadable(index: Int) {
    require(index in readerIndex() until writerIndex()) {
        "Index is outside of readable bytes: $index"
    }
}

fun ByteBuf.isEmpty(): Boolean = readableBytes() == 0

fun ByteBuf.isNotEmpty(): Boolean = !isEmpty()

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
fun ByteBuf.lastIndexOf(
    value: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): Int {
    requireReadable(fromIndex, toIndex)
    val valueLength = value.size
    val regionLength = toIndex - fromIndex
    if (regionLength < valueLength) return -1
    val factory = newKmpSearchProcessorFactory(value.reversedArray())
    return forEachByteDesc(fromIndex, regionLength, factory.newSearchProcessor())
}

@JvmOverloads
fun ByteBuf.lastIndexOf(
    value: String,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    charset: Charset = UTF_8
): Int = lastIndexOf(value.toByteArray(charset), fromIndex, toIndex)

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

fun ByteBuf.matches(value: ByteArray, atIndex: Int): Boolean {
    requireReadable(atIndex)
    if (atIndex + value.size > writerIndex()) return false

    value.forEachIndexed { index, byte ->
        if (getByte(index + atIndex) != byte) return false
    }

    return true
}

@JvmOverloads
fun ByteBuf.matches(
    value: String,
    atIndex: Int,
    charset: Charset = UTF_8
): Boolean = matches(value.toByteArray(charset), atIndex)

@JvmOverloads
fun ByteBuf.startsWith(
    value: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): Boolean {
    requireReadable(fromIndex, toIndex)
    if (toIndex - fromIndex < value.size) return false
    return matches(value, fromIndex)
}

@JvmOverloads
fun ByteBuf.startsWith(
    value: String,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    charset: Charset = UTF_8,
): Boolean = startsWith(value.toByteArray(charset), fromIndex, toIndex)

@JvmOverloads
fun ByteBuf.endsWith(
    value: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): Boolean {
    requireReadable(fromIndex, toIndex)
    val valueLength = value.size
    if (toIndex - fromIndex < valueLength) return false
    return matches(value, toIndex - valueLength)
}

@JvmOverloads
fun ByteBuf.endsWith(
    value: String,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    charset: Charset = UTF_8
): Boolean = endsWith(value.toByteArray(charset), fromIndex, toIndex)

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

private inline fun ByteBuf.removeValue(
    value: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    searchFunction: (value: ByteArray, fromIndex: Int, toIndex: Int) -> Int
): Boolean {
    requireReadable(fromIndex, toIndex)
    val atIndex = searchFunction(value, fromIndex, toIndex)
    if (atIndex < 0) return false
    remove(atIndex, atIndex + value.size)
    return true
}

@JvmOverloads
fun ByteBuf.remove(
    value: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): Boolean = removeValue(value, fromIndex, toIndex, ::indexOf)

@JvmOverloads
fun ByteBuf.remove(
    value: String,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    charset: Charset = UTF_8
): Boolean = remove(value.toByteArray(charset), fromIndex, toIndex)

@JvmOverloads
fun ByteBuf.removeLast(
    value: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): Boolean = removeValue(value, fromIndex, toIndex, ::lastIndexOf)

@JvmOverloads
fun ByteBuf.removeLast(
    value: String,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    charset: Charset = UTF_8
): Boolean = removeLast(value.toByteArray(charset), fromIndex, toIndex)

@JvmOverloads
fun ByteBuf.removeAll(
    value: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): Boolean {
    var toIndex = toIndex
    val valueSize = value.size
    var removed = false

    while (removeLast(value, fromIndex, toIndex)) {
        toIndex -= valueSize
        removed = true
    }

    return removed
}

@JvmOverloads
fun ByteBuf.removeAll(
    value: String,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    charset: Charset = UTF_8
): Boolean = removeAll(value.toByteArray(charset), fromIndex, toIndex)

private inline fun ByteBuf.replaceValue(
    source: ByteArray,
    target: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    searchFunction: (value: ByteArray, fromIndex: Int, toIndex: Int) -> Int
): Boolean {
    val sizeDiff = source.size - target.size
    val sourceIndex = searchFunction(source, fromIndex, toIndex)

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
    source: ByteArray,
    target: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): Boolean = replaceValue(source, target, fromIndex, toIndex, ::indexOf)

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

@JvmOverloads
fun ByteBuf.replaceLast(
    source: ByteArray,
    target: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): Boolean = replaceValue(source, target, fromIndex, toIndex, ::lastIndexOf)

@JvmOverloads
fun ByteBuf.replaceLast(
    source: String,
    target: String,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    charset: Charset = UTF_8
): Boolean = replaceLast(
    source.toByteArray(charset),
    target.toByteArray(charset),
    fromIndex,
    toIndex
)

@JvmOverloads
fun ByteBuf.replaceAll(
    source: ByteArray,
    target: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): Boolean {
    var toIndex = toIndex
    val valueSize = source.size
    var replaced = false

    while (replaceLast(source, target, fromIndex, toIndex)) {
        toIndex -= valueSize
        replaced = true
    }

    return replaced
}

@JvmOverloads
fun ByteBuf.replaceAll(
    source: String,
    target: String,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    charset: Charset = UTF_8
): Boolean = replaceAll(
    source.toByteArray(charset),
    target.toByteArray(charset),
    fromIndex,
    toIndex
)

fun ByteBuf.trimStart(
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    predicate: (Byte) -> Boolean = { it <= 32 }
): ByteBuf = apply {
    val startIndex = forEachByte(fromIndex, toIndex - fromIndex, predicate)
    if (startIndex > fromIndex) remove(fromIndex, startIndex)
}

@JvmOverloads
fun ByteBuf.trimStart(
    vararg values: Byte,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): ByteBuf = trimStart(fromIndex, toIndex) { it in values }

fun ByteBuf.trimEnd(
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    predicate: (Byte) -> Boolean = { it <= 32 }
): ByteBuf = apply {
    val endIndex = forEachByteDesc(fromIndex, toIndex - fromIndex, predicate)
    if (endIndex < toIndex - 1) remove(endIndex + 1, toIndex)
}

@JvmOverloads
fun ByteBuf.trimEnd(
    vararg values: Byte,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): ByteBuf = trimEnd(fromIndex, toIndex) { it in values }

fun ByteBuf.trim(
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    predicate: (Byte) -> Boolean = { it <= 32 }
): ByteBuf = apply {
    val regionLength = toIndex - fromIndex
    val startIndex = forEachByte(fromIndex, regionLength, predicate)
    val endIndex = forEachByteDesc(fromIndex, regionLength, predicate)
    if (endIndex < toIndex - 1) remove(endIndex + 1, toIndex)
    if (startIndex > fromIndex) remove(fromIndex, startIndex)
}

@JvmOverloads
fun ByteBuf.trim(
    vararg values: Byte,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): ByteBuf = trim(fromIndex, toIndex) { it in values }

@JvmOverloads
fun ByteBuf.padStart(
    length: Int,
    value: Byte = 0,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): ByteBuf = apply {
    val currentLength = toIndex - fromIndex
    if (currentLength >= length) return@apply
    val padding = ByteArray(length - currentLength) { value }
    insert(padding, fromIndex)
}

@JvmOverloads
fun ByteBuf.padEnd(
    length: Int,
    value: Byte = 0,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): ByteBuf = apply {
    val currentLength = toIndex - fromIndex
    if (currentLength >= length) return@apply
    val padding = ByteArray(length - currentLength) { value }
    insert(padding, toIndex)
}
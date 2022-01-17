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

private val EMPTY_ARRAY = ByteArray(0)
private const val EMPTY_STRING = ""

private fun Int.mapZeroOrNegative(toValue: Int) = if (this <= 0) toValue else this
private fun Int.mapNegative(toValue: Int): Int = if (this < 0) toValue else this

fun ByteBuf.asExpandable(): ByteBuf = when {
    !isReadOnly && maxCapacity() == Int.MAX_VALUE -> this
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

operator fun ByteBuf.get(index: Int): Byte = getByte(index)

operator fun ByteBuf.set(index: Int, value: Byte): ByteBuf = setByte(index, value.toInt())

fun ByteBuf.shiftReaderIndex(shift: Int): ByteBuf = readerIndex(readerIndex() + shift)

fun ByteBuf.shiftWriterIndex(shift: Int): ByteBuf = writerIndex(writerIndex() + shift)

fun ByteBuf.isEmpty(): Boolean = readableBytes() == 0

fun ByteBuf.isNotEmpty(): Boolean = !isEmpty()

@JvmOverloads
fun ByteBuf.indexOf(
    value: Byte,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
): Int {
    requireReadable(fromIndex, toIndex)
    return indexOf(fromIndex, toIndex, value)
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
fun ByteBuf.lastIndexOf(
    value: Byte,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
): Int {
    requireReadable(fromIndex, toIndex)
    return indexOf(toIndex, fromIndex, value)
}

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
    value: Byte,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): Boolean = indexOf(value, fromIndex, toIndex) >= 0

@JvmOverloads
fun ByteBuf.contains(
    value: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): Boolean = indexOf(value, fromIndex, toIndex) >= 0

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
    value: Byte,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): Boolean {
    requireReadable(fromIndex, toIndex)
    return this[fromIndex] == value
}

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
    value: Byte,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): Boolean {
    requireReadable(fromIndex, toIndex)
    return this[toIndex - 1] == value
}

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

@JvmOverloads
fun ByteBuf.bytesBefore(
    value: Byte,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
): Int = indexOf(value, fromIndex, toIndex) - fromIndex

@JvmOverloads
fun ByteBuf.bytesBefore(
    value: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
): Int = indexOf(value, fromIndex, toIndex) - fromIndex

@JvmOverloads
fun ByteBuf.bytesBefore(
    value: String,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    charset: Charset = UTF_8
): Int = bytesBefore(value.toByteArray(charset), fromIndex, toIndex)

@JvmOverloads
fun ByteBuf.bytesBeforeLast(
    value: Byte,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
): Int = lastIndexOf(value, fromIndex, toIndex) - fromIndex

@JvmOverloads
fun ByteBuf.bytesBeforeLast(
    value: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
): Int = lastIndexOf(value, fromIndex, toIndex) - fromIndex

@JvmOverloads
fun ByteBuf.bytesBeforeLast(
    value: String,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    charset: Charset = UTF_8
): Int = bytesBeforeLast(value.toByteArray(charset), fromIndex, toIndex)

@JvmOverloads
fun ByteBuf.bytesAfter(
    value: Byte,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
): Int {
    val index = indexOf(value, fromIndex, toIndex)
    if (index < 0) return -1
    return toIndex - index - 1
}

@JvmOverloads
fun ByteBuf.bytesAfter(
    value: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
): Int {
    val index = indexOf(value, fromIndex, toIndex)
    if (index < 0) return -1
    return toIndex - index - value.size
}

@JvmOverloads
fun ByteBuf.bytesAfter(
    value: String,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    charset: Charset = UTF_8
): Int = bytesAfter(value.toByteArray(charset), fromIndex, toIndex)

@JvmOverloads
fun ByteBuf.bytesAfterLast(
    value: Byte,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
): Int {
    val index = lastIndexOf(value, fromIndex, toIndex)
    if (index < 0) return -1
    return toIndex - index - 1
}

@JvmOverloads
fun ByteBuf.bytesAfterLast(
    value: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
): Int {
    val index = lastIndexOf(value, fromIndex, toIndex)
    if (index < 0) return -1
    return toIndex - index - value.size
}

@JvmOverloads
fun ByteBuf.bytesAfterLast(
    value: String,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    charset: Charset = UTF_8
): Int = bytesAfterLast(value.toByteArray(charset), fromIndex, toIndex)

private fun ByteBuf.shift(fromIndex: Int, shiftSize: Int) = apply {
    requireReadable(fromIndex)
    require(shiftSize > 0) { "Shift size is less or equal to zero: $shiftSize" }
    require(shiftSize <= maxWritableBytes()) { "Not enough free space to shift $shiftSize bytes: ${maxWritableBytes()}" }
    ensureWritable(shiftSize)
    setBytes(fromIndex + shiftSize, copy(fromIndex, writerIndex() - fromIndex))
    shiftWriterIndex(shiftSize)
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

    when (toIndex) {
        writerIndex() -> writerIndex(fromIndex)
        else -> {
            setBytes(fromIndex, slice(toIndex, writerIndex() - toIndex))
            shiftWriterIndex(-(toIndex - fromIndex))
        }
    }
}

private inline fun ByteBuf.remove(
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
): Boolean = remove(value, fromIndex, toIndex, ::indexOf)

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
): Boolean = remove(value, fromIndex, toIndex, ::lastIndexOf)

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

fun ByteBuf.replace(
    fromIndex: Int,
    toIndex: Int,
    value: ByteArray
): ByteBuf = apply {
    requireReadable(fromIndex, toIndex)

    val lengthDiff = (toIndex - fromIndex) - value.size

    when {
        lengthDiff < 0 -> shift(fromIndex, -lengthDiff)
        lengthDiff > 0 -> remove(fromIndex, fromIndex + lengthDiff)
    }

    setBytes(fromIndex, value)
}

@JvmOverloads
fun ByteBuf.replace(
    fromIndex: Int,
    toIndex: Int,
    value: String,
    charset: Charset = UTF_8
): ByteBuf = replace(fromIndex, toIndex, value.toByteArray(charset))

private inline fun ByteBuf.replace(
    source: ByteArray,
    target: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    searchFunction: (value: ByteArray, fromIndex: Int, toIndex: Int) -> Int
): Boolean {
    val sourceIndex = searchFunction(source, fromIndex, toIndex)
    if (sourceIndex < 0) return false
    replace(sourceIndex, sourceIndex + source.size, target)
    return true
}

@JvmOverloads
fun ByteBuf.replace(
    source: ByteArray,
    target: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): Boolean = replace(source, target, fromIndex, toIndex, ::indexOf)

@JvmOverloads
fun ByteBuf.replace(
    source: String,
    target: String,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    charset: Charset = UTF_8
): Boolean = replace(source.toByteArray(charset), target.toByteArray(charset), fromIndex, toIndex)

@JvmOverloads
fun ByteBuf.replaceLast(
    source: ByteArray,
    target: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): Boolean = replace(source, target, fromIndex, toIndex, ::lastIndexOf)

@JvmOverloads
fun ByteBuf.replaceLast(
    source: String,
    target: String,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    charset: Charset = UTF_8
): Boolean = replaceLast(source.toByteArray(charset), target.toByteArray(charset), fromIndex, toIndex)

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
): Boolean = replaceAll(source.toByteArray(charset), target.toByteArray(charset), fromIndex, toIndex)

@JvmOverloads
fun ByteBuf.trimStart(
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    predicate: (Byte) -> Boolean = { it <= 32 }
): ByteBuf = apply {
    requireReadable(fromIndex, toIndex)
    val startIndex = forEachByte(fromIndex, toIndex - fromIndex, predicate)
    if (startIndex > fromIndex) remove(fromIndex, startIndex)
}

@JvmOverloads
fun ByteBuf.trimStart(
    vararg values: Byte,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): ByteBuf = trimStart(fromIndex, toIndex) { it in values }

@JvmOverloads
fun ByteBuf.trimEnd(
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    predicate: (Byte) -> Boolean = { it <= 32 }
): ByteBuf = apply {
    requireReadable(fromIndex, toIndex)
    val endIndex = forEachByteDesc(fromIndex, toIndex - fromIndex, predicate)
    if (endIndex < toIndex - 1) remove(endIndex + 1, toIndex)
}

@JvmOverloads
fun ByteBuf.trimEnd(
    vararg values: Byte,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex()
): ByteBuf = trimEnd(fromIndex, toIndex) { it in values }

@JvmOverloads
fun ByteBuf.trim(
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    predicate: (Byte) -> Boolean = { it <= 32 }
): ByteBuf = apply {
    requireReadable(fromIndex, toIndex)
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
    requireReadable(fromIndex, toIndex)
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
    requireReadable(fromIndex, toIndex)
    val currentLength = toIndex - fromIndex
    if (currentLength >= length) return@apply
    val padding = ByteArray(length - currentLength) { value }
    insert(padding, toIndex)
}

@JvmOverloads
fun ByteBuf.subsequence(
    fromIndex: Int,
    toIndex: Int = writerIndex()
): ByteArray {
    requireReadable(fromIndex, toIndex)
    val length = toIndex - fromIndex
    if (length == 0) return EMPTY_ARRAY
    return ByteArray(length).apply { getBytes(fromIndex, this) }
}

@JvmOverloads
fun ByteBuf.substring(
    fromIndex: Int,
    toIndex: Int = writerIndex(),
    charset: Charset = UTF_8
): String = when (val value = subsequence(fromIndex, toIndex)) {
    EMPTY_ARRAY -> EMPTY_STRING
    else -> value.toString(charset)
}

@JvmOverloads
inline fun ByteBuf.forEachSlice(
    nextSlice: ByteBuf.() -> ByteBuf?,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    action: ByteBuf.() -> Unit
) {
    requireReadable(fromIndex, toIndex)

    asReadOnly().run {
        setIndex(fromIndex, toIndex)

        while (isReadable) {
            val readableBytes = readableBytes()
            val slice = nextSlice() ?: break
            check(readableBytes > readableBytes()) { "Slice function did not read anything" }
            slice.action()
        }
    }
}

@JvmOverloads
inline fun ByteBuf.forEachSubsequence(
    crossinline findDelimiter: ByteBuf.() -> Pair<Int, Int>?,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    action: ByteBuf.() -> Unit
) {
    val readSlice: ByteBuf.() -> ByteBuf = {
        val (bytesBefore, delimiterLength) = findDelimiter() ?: (readableBytes() to 0)
        readSlice(bytesBefore + delimiterLength).shiftWriterIndex(-delimiterLength)
    }

    forEachSlice(readSlice, fromIndex, toIndex, action)
}

inline fun ByteBuf.forEachSubsequence(
    delimiter: Byte,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    action: ByteBuf.() -> Unit
) {
    val findDelimiter: ByteBuf.() -> Pair<Int, Int>? = {
        val bytesBefore = bytesBefore(delimiter)
        if (bytesBefore < 0) null else bytesBefore to 1
    }

    forEachSubsequence(findDelimiter, fromIndex, toIndex, action)
}

inline fun ByteBuf.forEachSubsequence(
    delimiter: ByteArray,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    action: ByteBuf.() -> Unit
) {
    val findDelimiter: ByteBuf.() -> Pair<Int, Int>? = {
        val bytesBefore = bytesBefore(delimiter)
        if (bytesBefore < 0) null else bytesBefore to delimiter.size
    }

    forEachSubsequence(findDelimiter, fromIndex, toIndex, action)
}

inline fun ByteBuf.forEachSubsequence(
    delimiter: String,
    fromIndex: Int = readerIndex(),
    toIndex: Int = writerIndex(),
    charset: Charset = UTF_8,
    action: ByteBuf.() -> Unit
) = forEachSubsequence(delimiter.toByteArray(charset), fromIndex, toIndex, action)
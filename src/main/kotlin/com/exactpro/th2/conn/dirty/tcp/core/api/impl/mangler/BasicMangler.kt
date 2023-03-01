/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.conn.dirty.tcp.core.api.impl.mangler

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status.PASSED
import com.exactpro.th2.common.event.EventUtils.createMessageBean
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel
import com.exactpro.th2.conn.dirty.tcp.core.api.IMangler
import com.exactpro.th2.conn.dirty.tcp.core.api.impl.mangler.BasicManglerFactory.BasicManglerSettings
import com.exactpro.th2.conn.dirty.tcp.core.api.impl.mangler.ValueSelector.Companion.contains
import com.exactpro.th2.netty.bytebuf.util.indexOf
import com.exactpro.th2.netty.bytebuf.util.insert
import com.exactpro.th2.netty.bytebuf.util.matches
import com.exactpro.th2.netty.bytebuf.util.remove
import com.exactpro.th2.netty.bytebuf.util.replace
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.kotlin.KotlinFeature.NullIsSameAsDefault
import com.fasterxml.jackson.module.kotlin.KotlinModule.Builder
import com.fasterxml.jackson.module.kotlin.readValue
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil.prettyHexDump
import java.nio.charset.Charset

private const val RULE_NAME_PROPERTY = "rule-name"
private const val RULE_ACTIONS_PROPERTY = "rule-actions"

private val MAPPER = JsonMapper.builder()
    .addModule(Builder().configure(NullIsSameAsDefault, true).build())
    .build()

open class BasicMangler(private val context: BasicManglerSettings) : IMangler {
    private val rules = context.rules

    override fun onOutgoing(channel: IChannel, message: ByteBuf, metadata: MutableMap<String, String>): Event? {
        val rule = getRule(message, metadata) ?: return null
        val original = message.copy()
        val actions = rule.actions.filter { it.applyTo(message) }.ifEmpty { return null }

        postMangle(message)

        return Event.start().apply {
            name("Message mangled")
            type("Mangle")
            status(PASSED)

            bodyData(createMessageBean("Rule: ${rule.name}"))
            bodyData(createMessageBean("Message:"))
            bodyData(createMessageBean(prettyHexDump(original)))
            bodyData(createMessageBean("Actions:"))

            actions.forEach {
                bodyData(createMessageBean(it.toString()))
            }
        }
    }

    private fun getRule(message: ByteBuf, metadata: Map<String, String>): Rule? {
        metadata[RULE_NAME_PROPERTY]?.also { name ->
            return rules.find { it.name == name } ?: throw IllegalArgumentException("Unknown rule: $name")
        }

        metadata[RULE_ACTIONS_PROPERTY]?.also { yaml ->
            val actions = try {
                MAPPER.readValue<List<Action>>(yaml)
            } catch (e: Exception) {
                throw IllegalArgumentException("Invalid '$RULE_ACTIONS_PROPERTY' value: $yaml", e)
            }

            return Rule("custom", listOf(ValueSelector(i8 = 0)), actions)
        }

        return rules.find { it.matches(message) }
    }

    open fun postMangle(message: ByteBuf): Unit = Unit
}

data class Rule(
    val name: String,
    @JsonProperty("if-contains") val values: List<ValueSelector>,
    @JsonProperty("then") val actions: List<Action>,
) {
    init {
        check(name.isNotBlank()) { "rule name must not be empty" }
        check(values.isNotEmpty()) { "rule has no values: $name" }
        check(actions.isNotEmpty()) { "rule has no actions: $name" }
    }

    fun matches(buffer: ByteBuf): Boolean = values.all { it in buffer }
}

class ValueSelector(
    i8: Byte? = null,
    i16: Short? = null,
    i16be: Short? = null,
    i32: Int? = null,
    i32be: Int? = null,
    i64: Long? = null,
    i64be: Long? = null,
    f32: Float? = null,
    f32be: Float? = null,
    f64: Double? = null,
    f64be: Double? = null,
    str: String? = null,
    bytes: ByteArray? = null,
    charset: Charset = Charsets.UTF_8,
    @JsonProperty("at-offset") val offset: Int = -1,
) : ValueDefinition(i8, i16, i16be, i32, i32be, i64, i64be, f32, f32be, f64, f64be, str, bytes, charset) {
    fun find(buffer: ByteBuf): Int = when {
        offset < 0 -> buffer.indexOf(value)
        offset >= buffer.readableBytes() -> -1
        buffer.matches(value, offset + buffer.readerIndex()) -> offset + buffer.readerIndex()
        else -> -1
    }

    override fun toString(): String = when {
        offset < 0 -> super.toString()
        else -> "${super.toString()} at offset $offset"
    }

    companion object {
        operator fun ByteBuf.contains(value: ValueSelector): Boolean = value.find(this) != -1
    }
}

fun <T : Number> T.toByteArray(): ByteArray = when (this) {
    is Byte -> byteArrayOf(this)
    is Short -> ByteArray(Short.SIZE_BYTES) { (this.toInt() shr it * Byte.SIZE_BITS).toByte() }
    is Int -> ByteArray(Int.SIZE_BYTES) { (this shr it * Byte.SIZE_BITS).toByte() }
    is Long -> ByteArray(Long.SIZE_BYTES) { (this shr it * Byte.SIZE_BITS).toByte() }
    is Float -> toRawBits().toByteArray()
    is Double -> toRawBits().toByteArray()
    else -> error("Unsupported number type: ${this::class.qualifiedName}")
}

fun <T : Number> T.toByteArrayBE(): ByteArray = toByteArray().apply { reverse() }

@JsonInclude(NON_NULL)
open class ValueDefinition(
    val i8: Byte? = null,
    val i16: Short? = null,
    val i16be: Short? = null,
    val i32: Int? = null,
    val i32be: Int? = null,
    val i64: Long? = null,
    val i64be: Long? = null,
    val f32: Float? = null,
    val f32be: Float? = null,
    val f64: Double? = null,
    val f64be: Double? = null,
    val str: String? = null,
    val bytes: ByteArray? = null,
    charset: Charset = Charsets.UTF_8,
) {
    @JsonIgnore val value: ByteArray = when {
        i8 != null -> i8.toByteArray()
        i16 != null -> i16.toByteArray()
        i16be != null -> i16be.toByteArrayBE()
        i32 != null -> i32.toByteArray()
        i32be != null -> i32be.toByteArrayBE()
        i64 != null -> i64.toByteArray()
        i64be != null -> i64be.toByteArrayBE()
        f32 != null -> f32.toByteArray()
        f32be != null -> f32be.toByteArrayBE()
        f64 != null -> f64.toByteArray()
        f64be != null -> f64be.toByteArrayBE()
        str != null -> str.toByteArray(charset)
        bytes != null -> bytes
        else -> error("No value specified")
    }

    override fun toString(): String = when {
        i8 != null -> ("i8($i8)")
        i16 != null -> ("i16($i16)")
        i16be != null -> ("i16be($i16be)")
        i32 != null -> ("i32($i32)")
        i32be != null -> ("i32be($i32be)")
        i64 != null -> ("i64($i64)")
        i64be != null -> ("i64be($i64be)")
        f32 != null -> ("f32($f32)")
        f32be != null -> ("f32be($f32be)")
        f64 != null -> ("f64($f64)")
        f64be != null -> ("f64be($f64be)")
        str != null -> ("str($str)")
        bytes != null -> ("bytes(${bytes.joinToString()})")
        else -> error("No value specified")
    }
}

@JsonInclude(NON_NULL)
data class Action(
    val set: ValueSelector? = null,
    val add: ValueSelector? = null,
    val move: ValueSelector? = null,
    val replace: ValueSelector? = null,
    val remove: ValueSelector? = null,
    val with: ValueDefinition? = null,
    val before: ValueSelector? = null,
    val after: ValueSelector? = null,
) {
    init {
        val operations = listOfNotNull(set, add, move, remove, replace)

        require(operations.isNotEmpty()) { "Action must have at least one operation" }
        require(operations.size == 1) { "Action has more than one operation" }

        require(set == null && remove == null || with == null && before == null && after == null) {
            "'set'/'remove' operations cannot have 'with', 'before', 'after' options"
        }

        require(add == null && move == null || with == null) {
            "'add'/'move' operations cannot have 'with' option"
        }

        require(replace == null || with != null) {
            "'replace' operation requires 'with' option"
        }

        require(before == null || after == null) {
            "'before' and 'after' options are mutually exclusive"
        }

        require(move == null || before != null || after != null) {
            "'move' option requires 'before' or 'after' option"
        }

        require(set == null || set.offset >= 0) {
            "'set' operation requires a valid index"
        }

        require(add == null || (add.offset >= 0) xor (before != null || after != null)) {
            "'add' operation requires either a valid index or 'before'/'after' option"
        }
    }

    fun applyTo(buffer: ByteBuf): Boolean {
        set?.also { set ->
            val index = set.offset
            val value = set.value
            if (index + value.size > buffer.writerIndex()) return false
            buffer.setBytes(index, value)
            return true
        }

        add?.also { add ->
            before?.also { before ->
                val index = before.find(buffer)
                if (index < 0) return false
                buffer.insert(add.value, index)
                return true
            }

            after?.also { after ->
                val index = after.find(buffer)
                if (index < 0) return false
                buffer.insert(add.value, index + after.value.size)
                return true
            }

            if (add.offset >= 0) {
                val index = add.offset
                if (index > buffer.writerIndex()) return false
                buffer.insert(add.value, index)
                return true
            }
        }

        move?.also { move ->
            val moveIndex = move.find(buffer)

            if (moveIndex < 0) return false

            (before ?: after)?.also { anchor ->
                val anchorIndex = anchor.find(buffer)

                if (anchorIndex < 0) return false

                val value = move.value
                val targetIndex = if (after == null) anchorIndex else anchorIndex + anchor.value.size

                if (moveIndex < anchorIndex) {
                    buffer.insert(value, targetIndex)
                    buffer.remove(moveIndex, moveIndex + value.size)
                } else {
                    buffer.remove(moveIndex, moveIndex + value.size)
                    buffer.insert(value, targetIndex)
                }

                return true
            }
        }

        replace?.also { replace ->
            val index = replace.find(buffer)

            if (index < 0) return false

            with?.also { with ->
                buffer.replace(index, index + replace.value.size, with.value)
                return true
            }
        }

        remove?.also { remove ->
            val index = remove.find(buffer)
            if (index < 0) return false
            buffer.remove(index, index + remove.value.size)
            return true
        }

        return false
    }

    override fun toString(): String = buildString {
        set?.apply { append("set $this") }
        add?.apply { append("add $this") }
        move?.apply { append("move $this") }
        replace?.apply { append("replace $this") }
        remove?.apply { append("remove $this") }
        with?.apply { append(" with $this") }
        before?.apply { append(" before $this") }
        after?.apply { append(" after $this") }
    }
}

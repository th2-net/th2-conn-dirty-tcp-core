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

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.Unpooled
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TestBasicMangler {
    @Test fun `set value`() {
        val message = buffer("01 00 00 00 00 36 37 38 09")
        val rule = set(i32be(0x02030405) at 1) onlyIf contains(str("678"))
        assertTrue(rule.applyTo(message))
        assertEquals("01 02 03 04 05 36 37 38 09", message.toHexString())
    }

    @Test fun `add value`() {
        val message = buffer("01 02")
        val rule = add(bytes(0x03, 0x04) at 2) onlyIf contains(i8(0x02) at 1)
        assertTrue(rule.applyTo(message))
        assertEquals("01 02 03 04", message.toHexString())
    }

    @Test fun `add value before`() {
        val message = buffer("01 04")
        val rule = add(bytes(0x02, 0x03), before = i8(0x04) at 1) onlyIf contains(i8(0x04))
        assertTrue(rule.applyTo(message))
        assertEquals("01 02 03 04", message.toHexString())
    }

    @Test fun `add value after`() {
        val message = buffer("01 04")
        val rule = add(bytes(0x02, 0x03), after = i8(0x01) at 0) onlyIf contains(i8(0x01))
        assertTrue(rule.applyTo(message))
        assertEquals("01 02 03 04", message.toHexString())
    }

    @Test fun `move value before`() {
        val message = buffer("03 04 01 02")
        val rule = move(bytes(0x01, 0x02), before = i8(0x03) at 0) onlyIf contains(i8(0x03))
        assertTrue(rule.applyTo(message))
        assertEquals("01 02 03 04", message.toHexString())
    }

    @Test fun `move value after`() {
        val message = buffer("03 04 01 02")
        val rule = move(bytes(0x03, 0x04), after = i8(0x02) at 3) onlyIf contains(i8(0x01))
        assertTrue(rule.applyTo(message))
        assertEquals("01 02 03 04", message.toHexString())
    }

    @Test fun `replace value`() {
        val message = buffer("00 01 02 03 04 00")
        val rule = replace(i32be(0x01020304) at 1) with i32(0x01020304) onlyIf contains(bytes(0x02, 0x03))
        assertTrue(rule.applyTo(message))
        assertEquals("00 04 03 02 01 00", message.toHexString())
    }

    @Test fun `remove value`() {
        val message = buffer("01 00 00 80 40 02")
        val rule = remove(f32(4.0F) at 1) onlyIf contains(i8(0x40))
        assertTrue(rule.applyTo(message))
        assertEquals("01 02", message.toHexString())
    }

    companion object {
        private fun buffer(hex: String) = hex.split(' ').map { it.toUByte(16).toByte() }.toByteArray().run(Unpooled.buffer()::writeBytes)
        private fun ByteBuf.toHexString() = ByteBufUtil.hexDump(this).chunked(2).joinToString(" ")

        private fun i8(value: Byte) = ValueDefinition(i8 = value)
        private fun i32(value: Int) = ValueDefinition(i32 = value)
        private fun i32be(value: Int) = ValueDefinition(i32be = value)
        private fun f32(value: Float) = ValueDefinition(f32 = value)
        private fun str(value: String) = ValueDefinition(str = value)
        private fun bytes(vararg value: Byte) = ValueSelector(bytes = value)

        private infix fun ValueDefinition.at(offset: Int) = ValueSelector(bytes = value, offset = offset)
        private fun contains(value: ValueDefinition) = ValueSelector(bytes = value.value)

        private fun set(selector: ValueSelector) = Action(set = selector)
        private fun add(selector: ValueSelector, before: ValueSelector? = null, after: ValueSelector? = null) = Action(add = selector, before = before, after = after)
        private fun move(selector: ValueSelector, before: ValueSelector? = null, after: ValueSelector? = null) = Action(move = selector, before = before, after = after)
        private fun replace(selector: ValueSelector) = selector
        private fun remove(selector: ValueSelector) = Action(remove = selector)
        private infix fun ValueSelector.with(value: ValueDefinition) = Action(replace = this, with = value)
        private infix fun Action.onlyIf(selector: ValueSelector) = Rule("test", listOf(selector), listOf(this))

        private fun Rule.applyTo(buffer: ByteBuf) = matches(buffer) && actions.all { it.applyTo(buffer) }
    }
}
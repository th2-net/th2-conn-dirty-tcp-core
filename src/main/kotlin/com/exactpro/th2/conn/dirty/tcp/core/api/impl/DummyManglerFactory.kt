/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.conn.dirty.tcp.core.api.impl

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.conn.dirty.tcp.core.api.IContext
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolMangler
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolManglerFactory
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolManglerSettings
import io.netty.buffer.ByteBuf

object DummyManglerFactory : IProtocolManglerFactory {
    override val name: String = "dummy"
    override val settings: Class<out IProtocolManglerSettings> = DummyManglerSettings::class.java
    override fun create(context: IContext<IProtocolManglerSettings>): IProtocolMangler = DummyMangler

    class DummyManglerSettings : IProtocolManglerSettings

    object DummyMangler : IProtocolMangler {
        override fun onOutgoing(message: ByteBuf, metadata: Map<String, String>): Event? = null
    }
}

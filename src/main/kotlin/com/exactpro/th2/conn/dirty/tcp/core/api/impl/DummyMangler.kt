package com.exactpro.th2.conn.dirty.tcp.core.api.impl

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.conn.dirty.tcp.core.api.*
import io.netty.buffer.ByteBuf

class DummyManglerFactory: IProtocolManglerFactory {
    override val name: String = DummyManglerFactory::class.java.name

    override val settings: Class<out IProtocolManglerSettings> = DummyManglerSettings::class.java

    override fun create(context: IContext<IProtocolManglerSettings>): IProtocolMangler {
        return DummyMangler()
    }
}

class DummyManglerSettings: IProtocolManglerSettings{
    var username: String = "username"
    var password: String = "password"
}

class DummyMangler: IProtocolMangler{
    override fun onOutgoing(message: ByteBuf, metadata: Map<String, String>): Event? {
        return null
    }
}

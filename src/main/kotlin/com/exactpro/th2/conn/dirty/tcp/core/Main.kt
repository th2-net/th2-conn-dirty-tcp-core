/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

@file:JvmName("Main")

package com.exactpro.th2.conn.dirty.tcp.core

import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandlerFactory
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandlerSettings
import com.exactpro.th2.conn.dirty.tcp.core.api.IManglerFactory
import com.exactpro.th2.conn.dirty.tcp.core.api.IManglerSettings
import com.exactpro.th2.conn.dirty.tcp.core.api.impl.mangler.BasicManglerFactory
import com.exactpro.th2.conn.dirty.tcp.core.util.load
import com.exactpro.th2.conn.dirty.tcp.core.util.tryCatch
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinFeature.NullIsSameAsDefault
import com.fasterxml.jackson.module.kotlin.KotlinModule.Builder
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock
import kotlin.system.exitProcess

private val LOGGER = KotlinLogging.logger { }

fun main(args: Array<String>) = try {
    val resources = ConcurrentLinkedDeque<Pair<String, () -> Unit>>()

    Runtime.getRuntime().addShutdownHook(thread(start = false, name = "shutdown-hook") {
        resources.descendingIterator().forEach { (resource, destructor) ->
            LOGGER.debug { "Destroying resource: $resource" }
            tryCatch(destructor)
                .onSuccess { LOGGER.debug { "Successfully destroyed resource: $resource" } }
                .onFailure { LOGGER.error(it) { "Failed to destroy resource: $resource" } }
        }
    })

    val factory = tryCatch {
        CommonFactory.createFromArguments(*args)
    }.getOrElse {
        LOGGER.error(it) { "Failed to create common factory with arguments: ${args.joinToString(" ")}" }
        CommonFactory()
    }.apply { resources += "factory" to ::close }

    val handlerFactory = load<IHandlerFactory>()
    val manglerFactory = load<IManglerFactory?>() ?: run {
        LOGGER.warn { "No mangler was found. Using a default one" }
        BasicManglerFactory
    }

    LOGGER.info { "Loaded protocol handler factory: ${handlerFactory.name}" }
    LOGGER.info { "Loaded protocol mangler factory: ${manglerFactory.name}" }

    val module = SimpleModule()
        .addAbstractTypeMapping(IHandlerSettings::class.java, handlerFactory.settings)
        .addAbstractTypeMapping(IManglerSettings::class.java, manglerFactory.settings)

    val mapper = JsonMapper.builder()
        .addModule(Builder().configure(NullIsSameAsDefault, true).build())
        .addModule(JavaTimeModule())
        .addModule(module)
        .build()

    val settings = factory.getCustomConfiguration(Settings::class.java, mapper)
    val eventRouter = factory.eventBatchRouter
    val protoMessageRouter = factory.messageRouterMessageGroupBatch
    val transportMessageRouter = factory.transportGroupBatchRouter

    val boxName = factory.boxConfiguration.boxName
    require(boxName.isNotBlank()) { "Box name can't be blank." }
    val defaultBook = factory.boxConfiguration.bookName
    require(defaultBook.isNotBlank()) { "Default book name can't be blank." }

    Microservice(
        defaultBook,
        boxName,
        settings,
        factory::readDictionary,
        eventRouter,
        protoMessageRouter,
        transportMessageRouter,
        handlerFactory,
        manglerFactory,
        factory.grpcRouter
    ) { resource, destructor ->
        resources += resource to destructor
    }.run()

    LOGGER.info { "Successfully started" }

    ReentrantLock().run {
        val condition = newCondition()
        resources += "await-shutdown" to { withLock(condition::signalAll) }
        withLock(condition::await)
    }

    LOGGER.info { "Finished running" }
} catch (e: Exception) {
    LOGGER.error(e) { "Uncaught exception. Shutting down" }
    exitProcess(1)
}

data class SessionSettings(
    val sessionAlias: String,
    val sessionGroup: String = sessionAlias,
    val bookName: String? = null,
    val handler: IHandlerSettings,
    val mangler: IManglerSettings? = null,
) {
    init {
        bookName?.run { require(isNotBlank()) { "Custom book name shouldn't be blank." } }
        require(sessionAlias.isNotBlank()) { "'${::sessionAlias.name}' is blank" }
        require(sessionGroup.isNotBlank()) { "'${::sessionGroup.name}' is blank" }
    }
}

data class Settings(
    val sessions: List<SessionSettings>,
    val ioThreads: Int = sessions.size,
    val appThreads: Int = sessions.size * 2,
    val maxBatchSize: Int = 1000,
    val maxFlushTime: Long = 1000,
    //FIXME: would be better to remove this option because cradle group concept doesn't support publishing one group from several sources
    // and router for transport protocol doesn't support batch split
    val batchByGroup: Boolean = true,
    val publishSentEvents: Boolean = true,
    val publishConnectEvents: Boolean = true,
    val sendLimit: Long = 0,
    val receiveLimit: Long = 0,
    val useTransport: Boolean = false,
) {
    init {
        require(sessions.isNotEmpty()) { "'${::sessions.name}' is empty" }
        require(ioThreads > 0) { "'${::ioThreads.name}' must be positive" }
        require(appThreads > 0) { "'${::appThreads.name}' must be positive" }
        require(maxBatchSize > 0) { "'${::maxBatchSize.name}' must be positive" }
        require(maxFlushTime > 0) { "'${::maxFlushTime.name}' must be positive" }
        require(sendLimit >= 0) { "'${::sendLimit.name}' cannot be negative" }
        require(receiveLimit >= 0) { "'${::receiveLimit.name}' cannot be negative" }

        val duplicates = sessions.asSequence()
            .map { it.sessionAlias }
            .groupingBy { it }
            .eachCount()
            .asSequence()
            .filter { it.value > 1 }
            .map { it.key }
            .joinToString()

        require(duplicates.isEmpty()) { "Duplicate session aliases: $duplicates" }
    }
}

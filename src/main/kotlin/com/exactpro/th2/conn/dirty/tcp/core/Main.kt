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

@file:JvmName("Main")

package com.exactpro.th2.conn.dirty.tcp.core

import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandlerFactory
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandlerSettings
import com.exactpro.th2.conn.dirty.tcp.core.api.IManglerFactory
import com.exactpro.th2.conn.dirty.tcp.core.api.IManglerSettings
import com.exactpro.th2.conn.dirty.tcp.core.api.impl.DummyManglerFactory
import com.exactpro.th2.conn.dirty.tcp.core.util.load
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import mu.KotlinLogging
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
            runCatching(destructor).apply {
                onSuccess { LOGGER.debug { "Successfully destroyed resource: $resource" } }
                onFailure { LOGGER.error(it) { "Failed to destroy resource: $resource" } }
            }
        }
    })

    val factory = runCatching {
        CommonFactory.createFromArguments(*args)
    }.getOrElse {
        LOGGER.error(it) { "Failed to create common factory with arguments: ${args.joinToString(" ")}" }
        CommonFactory()
    }.apply { resources += "factory" to ::close }

    val handlerFactory = load<IHandlerFactory>()
    val manglerFactory = load<IManglerFactory?>() ?: run {
        LOGGER.warn { "No mangler was found. Using a dummy one" }
        DummyManglerFactory
    }

    LOGGER.info { "Loaded protocol handler factory: ${handlerFactory.name}" }
    LOGGER.info { "Loaded protocol mangler factory: ${manglerFactory.name}" }

    val module = SimpleModule()
        .addAbstractTypeMapping(IHandlerSettings::class.java, handlerFactory.settings)
        .addAbstractTypeMapping(IManglerSettings::class.java, manglerFactory.settings)

    val mapper = JsonMapper.builder()
        .addModule(KotlinModule(nullIsSameAsDefault = true))
        .addModule(module)
        .build()

    val settings = factory.getCustomConfiguration(Settings::class.java, mapper)
    val eventRouter = factory.eventBatchRouter
    val messageRouter = factory.messageRouterMessageGroupBatch

    Microservice(
        factory.rootEventId,
        settings,
        factory::readDictionary,
        eventRouter,
        messageRouter,
        handlerFactory,
        manglerFactory
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
    val handler: IHandlerSettings,
    val mangler: IManglerSettings,
) {
    init {
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
    val batchByGroup: Boolean = true,
    val publishSentEvents: Boolean = true,
    val publishConnectEvents: Boolean = true,
) {
    init {
        require(sessions.isNotEmpty()) { "'${::sessions.name}' is empty" }
        require(ioThreads > 0) { "'${::ioThreads.name}' must be positive" }
        require(appThreads > 0) { "'${::appThreads.name}' must be positive" }
        require(maxBatchSize > 0) { "'${::maxBatchSize.name}' must be positive" }
        require(maxFlushTime > 0) { "'${::maxFlushTime.name}' must be positive" }

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

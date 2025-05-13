/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.file

import io.airbyte.cdk.load.command.DestinationCatalog
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.config.PipelineInputEvent
import io.airbyte.cdk.load.message.CheckpointMessage
import io.airbyte.cdk.load.message.DestinationMessage
import io.airbyte.cdk.load.message.DestinationStreamAffinedMessage
import io.airbyte.cdk.load.message.PipelineHeartbeat
import io.airbyte.cdk.load.message.Undefined
import io.airbyte.cdk.load.state.PipelineEventBookkeepingRouter
import io.airbyte.cdk.load.state.ReservationManager
import io.airbyte.cdk.load.util.use
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.withTimeoutOrNull

class SocketInputFlow(
    private val catalog: DestinationCatalog,
    private val socket: ClientSocket,
    private val inputFormatReader: DataChannelReader,
    private val pipelineEventBookkeepingRouter: PipelineEventBookkeepingRouter,
    private val memoryManager: ReservationManager,
    private val maximumMessageSizeBytes: Int,
    private val socketReadTimeoutMs: Long,
    private val socketTimeoutBackoffMs: Long,
) : Flow<PipelineInputEvent> {
    private val log = KotlinLogging.logger {}

    sealed interface ReadResult
    @JvmInline value class Event(val event: PipelineInputEvent) : ReadResult
    data object EOF : ReadResult

    override suspend fun collect(collector: FlowCollector<PipelineInputEvent>) {
        pipelineEventBookkeepingRouter.use {
            socket.connect { inputStream ->
                val unopenedStreams = catalog.streams.map { it.descriptor }.toMutableSet()

                // Mark the input stream in case of timeout.
                inputStream.mark(maximumMessageSizeBytes)
                var iterator = inputFormatReader.read(inputStream)

                while (true) {
                    // Timeout the read if it takes too long.
                    val result = withTimeoutOrNull(socketReadTimeoutMs) {
                        if (iterator.hasNext()) {
                            val event = toPipelineEvent(iterator.next(), unopenedStreams)
                            Event(event ?: PipelineHeartbeat()).also {
                                inputStream.mark(maximumMessageSizeBytes)
                            }
                        } else {
                            EOF
                        }
                    } ?: run {
                        // If the read times out, we need to reset the stream and re-read.
                        inputStream.reset()
                        log.warn { "Socket read timeout. Sleeping for $socketTimeoutBackoffMs ms" }
                        delay(socketTimeoutBackoffMs)
                        inputStream.mark(maximumMessageSizeBytes)
                        iterator = inputFormatReader.read(inputStream)
                        Event(PipelineHeartbeat())
                    }

                    when (result) {
                        is Event -> collector.emit(result.event)
                        is EOF -> {
                            log.info { "End of file for ${socket.socketPath} reached." }
                            break
                        }
                    }
                }
            }
        }
    }

    private suspend fun toPipelineEvent(
        message: DestinationMessage,
        unopenedStreams: MutableSet<DestinationStream.Descriptor>
    ): PipelineInputEvent? {
        when (message) {
            is DestinationStreamAffinedMessage -> {
                val event =
                    pipelineEventBookkeepingRouter.handleStreamMessage(
                        message,
                        unopenedStreams = unopenedStreams
                    )
                return event
            }

            is CheckpointMessage ->
                pipelineEventBookkeepingRouter.handleCheckpoint(
                    memoryManager.reserve(message.serializedSizeBytes, message)
                )

            Undefined -> log.warn { "Unhandled message: $message" }
        }

        return null
    }
}

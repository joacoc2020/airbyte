/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.file

import io.airbyte.cdk.load.command.DestinationCatalog
import io.airbyte.cdk.load.message.DestinationMessage
import io.airbyte.cdk.load.message.DestinationMessageFactory
import io.airbyte.cdk.load.util.Jsons
import io.airbyte.protocol.models.v0.AirbyteMessage
import java.io.InputStream
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map

class JSONLDataChannelReader(catalog: DestinationCatalog) : DataChannelReader {
    // NOTE: Presumes that legacy file transfer is not compatible with sockets.
    private val destinationMessageFactory: DestinationMessageFactory =
        DestinationMessageFactory(catalog, fileTransferEnabled = false)
    private var bytesRead: Long = 0

    override fun read(inputStream: InputStream): Iterator<DestinationMessage> = iterator {
        val parser = Jsons.factory.createParser(inputStream)
        Jsons.readerFor(AirbyteMessage::class.java)
            .readValues<AirbyteMessage>(parser)
            .forEach {
                val serializedSize = parser.currentLocation().byteOffset - bytesRead
                bytesRead += serializedSize
                val message = destinationMessageFactory.fromAirbyteMessage(it, serializedSize)
                yield(message)
            }
    }
}

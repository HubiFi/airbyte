/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.integrations.destination.postgres.typing_deduping

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.db.jdbc.JdbcDatabase
import io.airbyte.cdk.integrations.base.JavaBaseConstants
import io.airbyte.cdk.integrations.destination.jdbc.ColumnDefinition
import io.airbyte.cdk.integrations.destination.jdbc.TableDefinition
import io.airbyte.integrations.base.destination.typing_deduping.*
import io.airbyte.cdk.integrations.destination.jdbc.typing_deduping.JdbcDestinationHandler
import io.airbyte.commons.exceptions.ConfigErrorException
import io.airbyte.integrations.base.destination.typing_deduping.AirbyteProtocolType
import io.airbyte.integrations.base.destination.typing_deduping.AirbyteType
import io.airbyte.integrations.base.destination.typing_deduping.Array
import io.airbyte.integrations.base.destination.typing_deduping.Sql
import io.airbyte.integrations.base.destination.typing_deduping.Struct
import io.airbyte.integrations.base.destination.typing_deduping.Union
import io.airbyte.integrations.base.destination.typing_deduping.UnsupportedOneOf
import io.airbyte.integrations.destination.postgres.PostgresGenerationHandler
import org.jooq.SQLDialect

class PostgresDestinationHandler(
    databaseName: String?,
    jdbcDatabase: JdbcDatabase,
    rawTableSchema: String,
    generationHandler: PostgresGenerationHandler,
) :
    JdbcDestinationHandler<PostgresState>(
        databaseName,
        jdbcDatabase,
        rawTableSchema,
        SQLDialect.POSTGRES,
        generationHandler = generationHandler
    ) {
    override fun toJdbcTypeName(airbyteType: AirbyteType): String {
        // This is mostly identical to the postgres implementation, but swaps jsonb to super
        if (airbyteType is AirbyteProtocolType) {
            return toJdbcTypeName(airbyteType)
        }
        return when (airbyteType.typeName) {
            Struct.TYPE,
            UnsupportedOneOf.TYPE,
            Array.TYPE -> "jsonb"
            Union.TYPE -> toJdbcTypeName((airbyteType as Union).chooseType())
            else -> throw IllegalArgumentException("Unsupported AirbyteType: $airbyteType")
        }
    }

    override fun toDestinationState(json: JsonNode): PostgresState {
        return PostgresState(
            json.hasNonNull("needsSoftReset") && json["needsSoftReset"].asBoolean(),
            json.hasNonNull("isAirbyteMetaPresentInRaw") &&
                json["isAirbyteMetaPresentInRaw"].asBoolean(),
            json.hasNonNull("isAirbyteGenerationIdPresent") &&
                json["isAirbyteGenerationIdPresent"].asBoolean()
        )
    }

    override fun createNamespaces(schemas: Set<String>) {
        TODO("Not yet implemented")
    }

    override fun existingSchemaMatchesStreamConfig(
        stream: StreamConfig?,
        existingTable: TableDefinition
    ): Boolean {
        // Check that the columns match, with special handling for the metadata columns.
        if (
            !(existingTable.columns.containsKey(JavaBaseConstants.COLUMN_NAME_AB_RAW_ID) &&
                isAirbyteRawIdColumnMatch(existingTable)) ||
                !(existingTable.columns.containsKey(
                    JavaBaseConstants.COLUMN_NAME_AB_EXTRACTED_AT
                ) && isAirbyteExtractedAtColumnMatch(existingTable)) ||
                !(existingTable.columns.containsKey(JavaBaseConstants.COLUMN_NAME_AB_META) &&
                    isAirbyteMetaColumnMatch(existingTable)) ||
                (columns == DestinationColumns.V2_WITH_GENERATION &&
                    !(existingTable.columns.containsKey(
                        JavaBaseConstants.COLUMN_NAME_AB_GENERATION_ID
                    ) && isAirbyteGenerationColumnMatch(existingTable)))
        ) {
            // Missing AB meta columns from final table, we need them to do proper T+D so trigger
            // soft-reset
            return false
        }
        val intendedColumns =
            LinkedHashMap(
                stream!!.columns.entries.associate { it.key.name to toJdbcTypeName(it.value) }
            )

        // Filter out Meta columns since they don't exist in stream config.
        val actualColumns = LinkedHashMap<String?, String>()
        existingTable.columns.entries
            .filter { column: Map.Entry<String?, ColumnDefinition> ->
                JavaBaseConstants.V2_FINAL_TABLE_METADATA_COLUMNS.none { it == column.key }
            }
            .forEach { actualColumns[it.key] = it.value.type.lowercase() }

        return actualColumns == intendedColumns
    }

    private fun toJdbcTypeName(airbyteProtocolType: AirbyteProtocolType): String {
        return when (airbyteProtocolType) {
            AirbyteProtocolType.STRING -> "varchar"
            AirbyteProtocolType.NUMBER -> "numeric"
            AirbyteProtocolType.INTEGER -> "int8"
            AirbyteProtocolType.BOOLEAN -> "bool"
            AirbyteProtocolType.TIMESTAMP_WITH_TIMEZONE -> "timestamptz"
            AirbyteProtocolType.TIMESTAMP_WITHOUT_TIMEZONE -> "timestamp"
            AirbyteProtocolType.TIME_WITH_TIMEZONE -> "timetz"
            AirbyteProtocolType.TIME_WITHOUT_TIMEZONE -> "time"
            AirbyteProtocolType.DATE -> "date"
            AirbyteProtocolType.UNKNOWN -> "jsonb"
        }
    }

    override fun execute(sql: Sql) {
        try {
            super.execute(sql)
        } catch (e: Exception) {
            // executing the
            // DROP TABLE command.
            if (
                e.message!!.contains("ERROR: cannot drop table") &&
                    e.message!!.contains("because other objects depend on it")
            ) {
                throw ConfigErrorException(
                    "Failed to drop table without the CASCADE option. Consider changing the drop_cascade configuration parameter",
                    e
                )
            }
            throw e
        }
    }
}

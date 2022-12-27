package org.apache.flink.connector.clickhouse.util;

import org.apache.flink.connector.clickhouse.internal.common.DistributedEngineFullSchema;
import org.apache.flink.connector.clickhouse.internal.common.TableFullSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import org.apache.http.client.utils.URIBuilder;
import ru.yandex.clickhouse.ClickHouseConnection;

import javax.annotation.Nullable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.connector.clickhouse.config.ClickHouseConfig.PROPERTIES_PREFIX;

/** clickhouse util. */
public class ClickHouseUtil {

    public static final String EMPTY = "";

    private static final LocalDate DATE_PREFIX_OF_TIME = LocalDate.ofEpochDay(1);

    private static final Pattern DISTRIBUTED_TABLE_ENGINE_PATTERN =
            Pattern.compile(
                    "Distributed\\((?<cluster>[a-zA-Z_][0-9a-zA-Z_]*),\\s*(?<database>[a-zA-Z_][0-9a-zA-Z_]*),\\s*(?<table>[a-zA-Z_][0-9a-zA-Z_]*)");

    private static final String QUERY_TABLE_ENGINE_SQL =
            "SELECT engine_full FROM system.tables WHERE database = ? AND name = ?";

    public static String getJdbcUrl(String url, @Nullable String database) {
        try {
            database = database != null ? database : "";
            return "jdbc:" + (new URIBuilder(url)).setPath("/" + database).build().toString();
        } catch (Exception e) {
            throw new IllegalStateException(String.format("Cannot parse url: %s", url), e);
        }
    }

    public static DistributedEngineFullSchema getAndParseDistributedEngineSchema(
            ClickHouseConnection connection, String databaseName, String tableName)
            throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(QUERY_TABLE_ENGINE_SQL)) {
            stmt.setString(1, databaseName);
            stmt.setString(2, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String engineFull = rs.getString("engine_full");
                    Matcher matcher =
                            DISTRIBUTED_TABLE_ENGINE_PATTERN.matcher(engineFull.replace("'", ""));
                    if (matcher.find()) {
                        String cluster = matcher.group("cluster");
                        String database = matcher.group("database");
                        String table = matcher.group("table");
                        return DistributedEngineFullSchema.of(cluster, database, table);
                    } else {
                        return null;
                    }
                }
            }
        }

        throw new SQLException(
                String.format("table `%s`.`%s` does not exist", databaseName, tableName));
    }

    public static TableFullSchema getAndParseTableEngineSchema(
            ClickHouseConnection connection,
            String catalogName,
            String databasesNamed,
            String tableName) throws CatalogException {
        final String sql = "SELECT engine,engine_full FROM `system`.`tables` WHERE `database` = ? AND `name` = ?";
        try (PreparedStatement stm = connection.prepareStatement(sql)) {
            stm.setString(1, databasesNamed);
            stm.setString(2, tableName);
            try (ResultSet result = stm.executeQuery()) {
                if (result.next()) {
                    final String engineFull = result.getString("engine_full");
                    final String engine = result.getString("engine");
                    final TableFullSchema schema = new TableFullSchema(
                            engine,
                            databasesNamed,
                            tableName
                    );
                    final Matcher matcher =
                            DISTRIBUTED_TABLE_ENGINE_PATTERN
                                    .matcher(engineFull.replace("'", ""));
                    if (matcher.find()) {
                        schema.setDistributed(true);
                        schema.setCluster(matcher.group("cluster"));
                        schema.setDatabase(matcher.group("database"));
                        schema.setTable(matcher.group("table"));
                    }
                    return schema;
                }
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed getting engine full to %s.%s.%s",
                            catalogName,
                            databasesNamed,
                            tableName), e
            );
        }
        throw new CatalogException(
                String.format(
                        "table %s.%s.%s does not exist",
                        catalogName,
                        databasesNamed,
                        tableName
                )
        );
    }

    public static Properties getClickHouseProperties(Map<String, String> tableOptions) {
        final Properties properties = new Properties();

        tableOptions.keySet().stream()
                .filter(key -> key.startsWith(PROPERTIES_PREFIX))
                .forEach(
                        key -> {
                            final String value = tableOptions.get(key);
                            final String subKey = key.substring((PROPERTIES_PREFIX).length());
                            properties.setProperty(subKey, value);
                        });
        return properties;
    }

    public static Timestamp toEpochDayOneTimestamp(LocalTime localTime) {
        LocalDateTime localDateTime = localTime.atDate(DATE_PREFIX_OF_TIME);
        return Timestamp.valueOf(localDateTime);
    }

    public static String quoteIdentifier(String identifier) {
        return String.join(EMPTY, "`", identifier, "`");
    }
}

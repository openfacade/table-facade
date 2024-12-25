/*
 * Copyright 2024 OpenFacade Authors
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

package io.github.openfacade.table.spring.reactive.opengauss;

import io.github.openfacade.table.api.ComparisonCondition;
import io.github.openfacade.table.api.Condition;
import io.github.openfacade.table.spring.core.ReactiveBaseTableOperations;
import io.github.openfacade.table.spring.core.TableMetadata;
import io.github.openfacade.table.spring.util.TableMetadataUtil;
import io.r2dbc.spi.Row;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
@RequiredArgsConstructor
public class ReactiveOpenGaussTableOperations extends ReactiveBaseTableOperations {
    private final DatabaseClient databaseClient;

    public <T> Mono<T> insertOnDuplicateKeyUpdate(T object, Object[] pairs) {
        Class<?> type = object.getClass();
        classMap.putIfAbsent(type, TableMetadataUtil.parseClass(type));
        TableMetadata metadata = classMap.get(type);

        String tableName = escapeIdentifier(metadata.getTableName());

        Map<String, Object> parameters = metadata.getGetterMap().entrySet().stream()
                .map(entry -> {
                    try {
                        return new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().invoke(object));
                    } catch (Exception e) {
                        throw new RuntimeException("Error invoking getter", e);
                    }
                })
                .filter(entry -> entry.getValue() != null) // Filter out null values
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (parameters.isEmpty()) {
            throw new IllegalArgumentException("Cannot insert an object with all fields as null.");
        }

        List<String> columns = parameters.keySet().stream().toList();
        List<String> escapedColumns = columns.stream().map(this::escapeIdentifier).collect(Collectors.toList());
        String placeholders = columns.stream().map(col -> "?").collect(Collectors.joining(", "));

        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException("Pairs must contain an even number of elements (key-value pairs).");
        }

        String onDuplicateKeyUpdateClause = IntStream.range(0, pairs.length / 2)
                .mapToObj(i -> escapeIdentifier((String) pairs[2 * i]) + " = ?")
                .collect(Collectors.joining(", "));

        String query = String.format(
                "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
                tableName,
                String.join(", ", escapedColumns),
                placeholders,
                onDuplicateKeyUpdateClause
        );

        DatabaseClient.GenericExecuteSpec spec = databaseClient.sql(query);

        int index = 0;
        for (String column : columns) {
            spec = spec.bind(index++, parameters.get(column));
        }

        for (int i = 1; i < pairs.length; i += 2) {
            spec = spec.bind(index++, pairs[i]);
        }

        return spec.fetch().rowsUpdated().thenReturn(object);
    }

    @Override
    public <T> Mono<T> insert(T object, TableMetadata metadata) {
        String tableName = escapeIdentifier(metadata.getTableName());

        Map<String, Object> parameters = metadata.getGetterMap().entrySet().stream()
                .map(entry -> {
                    try {
                        return new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().invoke(object));
                    } catch (Exception e) {
                        throw new RuntimeException("Error invoking getter", e);
                    }
                })
                .filter(entry -> entry.getValue() != null) // Filter out null values
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (parameters.isEmpty()) {
            throw new IllegalArgumentException("Cannot insert an object with all fields as null.");
        }

        List<String> columns = parameters.keySet().stream().toList();

        List<String> escapedColumns = columns.stream().map(this::escapeIdentifier)
                .collect(Collectors.toList());

        String placeholders = columns.stream()
                .map(col -> "?")
                .collect(Collectors.joining(", "));

        String query = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, String.join(", ", escapedColumns), placeholders);

        DatabaseClient.GenericExecuteSpec spec = databaseClient.sql(query);
        for (int i = 0; i < columns.size(); i++) {
            spec = spec.bind(i, parameters.get(columns.get(i)));
        }
        return spec.fetch().rowsUpdated().thenReturn(object);
    }

    @Override
    public <T> Mono<Long> update(Condition condition, Object[] pairs, Class<T> type, TableMetadata metadata) {
        String tableName = escapeIdentifier(metadata.getTableName());
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException("Pairs must be an even number.");
        }

        String setClause = IntStream.range(0, pairs.length / 2)
                .mapToObj(i -> escapeIdentifier((String) pairs[2 * i]) + " = ?")
                .collect(Collectors.joining(", "));

        String query = "UPDATE " + tableName + " SET " + setClause + " WHERE " + condition(condition);

        DatabaseClient.GenericExecuteSpec spec = databaseClient.sql(query);
        for (int i = 1; i < pairs.length; i += 2) {
            spec = spec.bind(i / 2, pairs[i]);
        }

        return spec.fetch().rowsUpdated().map(Long::valueOf);
    }

    @Override
    public <T> Mono<T> find(Condition condition, Class<T> type, TableMetadata metadata) {
        String tableName = escapeIdentifier(metadata.getTableName());
        List<String> escapedColumns = metadata.getSetterMap().keySet().stream()
                .map(this::escapeIdentifier)
                .collect(Collectors.toList());

        String query = "SELECT " + String.join(", ", escapedColumns) + " FROM " + tableName + " WHERE " + condition(condition);

        return databaseClient.sql(query)
                .map((row, metadataAccessor) -> mapRowToEntity(row, type, metadata))
                .one();
    }

    @Override
    public <T> Flux<T> findAll(Class<T> type, TableMetadata metadata) {
        String tableName = escapeIdentifier(metadata.getTableName());
        List<String> escapedColumns = metadata.getSetterMap().keySet().stream()
                .map(this::escapeIdentifier)
                .collect(Collectors.toList());

        String query = "SELECT " + String.join(", ", escapedColumns) + " FROM " + tableName;

        return databaseClient.sql(query)
                .map((row, metadataAccessor) -> mapRowToEntity(row, type, metadata))
                .all();
    }

    private <T> T mapRowToEntity(Row row, Class<T> type, TableMetadata metadata) {
        T instance;
        try {
            instance = type.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Error creating instance of: " + type.getName(), e);
        }

        for (Map.Entry<String, Method> entry : metadata.getSetterMap().entrySet()) {
            String columnName = entry.getKey();
            Method setter = entry.getValue();
            Class<?> parameterType = setter.getParameterTypes()[0];

            try {
                setter.invoke(instance, row.get(columnName, parameterType));
            } catch (Exception e) {
                // fall back, opengauss map mysql blob to driver string, but it can't map string to bytes
                Object object = row.get(columnName, Object.class);
                if ((object instanceof String str) && parameterType == byte[].class) {
                    try {
                        setter.invoke(instance, HexFormat.of().parseHex(str));
                    } catch (IllegalAccessException ex) {
                        throw new RuntimeException(ex);
                    } catch (InvocationTargetException ex) {
                        throw new RuntimeException(ex);
                    }
                    continue;
                }
                throw new RuntimeException(
                        "Error setting field '" + columnName + "' for entity: " + type.getName(), e
                );
            }
        }

        return instance;
    }

    @Override
    public <T> Mono<Long> delete(Condition condition, Class<T> type, TableMetadata metadata) {
        String tableName = escapeIdentifier(metadata.getTableName());
        String query = "DELETE FROM " + tableName + " WHERE " + condition(condition);

        return databaseClient.sql(query)
                .fetch()
                .rowsUpdated()
                .map(Long::valueOf);
    }

    @Override
    public <T> Mono<Long> deleteAll(Class<T> type, TableMetadata metadata) {
        String tableName = escapeIdentifier(metadata.getTableName());
        String query = "DELETE FROM " + tableName;

        return databaseClient.sql(query)
                .fetch()
                .rowsUpdated()
                .map(Long::valueOf);
    }

    private String condition(Condition condition) {
        if (condition instanceof ComparisonCondition comparisonCondition) {
            return escapeIdentifier(comparisonCondition.getColumn()) + " " + comparisonCondition.getOperator().symbol() + " " + escapeValue(comparisonCondition.getValue());
        } else {
            throw new IllegalArgumentException("Unsupported condition type: " + condition.getClass().getName());
        }
    }

    private String escapeIdentifier(@NotNull String identifier) {
        return "`" + identifier + "`";
    }

    private String escapeValue(@Nullable Object value) {
        if (value == null) {
            return "NULL";
        }

        if (value instanceof String) {
            return "'" + ((String) value).replace("\\", "\\\\").replace("'", "\\'") + "'";
        }

        if (value instanceof Boolean) {
            return (Boolean) value ? "1" : "0";
        }

        if (value instanceof Number) {
            return value.toString();
        }

        return "'" + value.toString().replace("\\", "\\\\").replace("'", "\\'") + "'";
    }
}

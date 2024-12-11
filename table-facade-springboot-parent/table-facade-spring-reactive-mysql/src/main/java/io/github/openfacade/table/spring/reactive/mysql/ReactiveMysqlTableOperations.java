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

package io.github.openfacade.table.spring.reactive.mysql;

import io.github.openfacade.table.spring.core.ReactiveBaseTableOperations;
import io.github.openfacade.table.spring.core.TableMetadata;
import io.r2dbc.spi.Row;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ReactiveMysqlTableOperations extends ReactiveBaseTableOperations {
    private final DatabaseClient databaseClient;

    @Override
    public <T> Mono<T> insert(T object, TableMetadata metadata) {
        String tableName = escapeIdentifier(metadata.getTableName());

        Map<String, Object> parameters = metadata.getGetterMap().entrySet().stream()
                .map(entry -> {
                    try {
                        return Map.entry(entry.getKey(), entry.getValue().invoke(object));
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
        try {
            T instance = type.getDeclaredConstructor().newInstance();

            for (Map.Entry<String, Method> entry : metadata.getSetterMap().entrySet()) {
                String columnName = entry.getKey();
                Method setter = entry.getValue();
                Class<?> parameterType = setter.getParameterTypes()[0];

                setter.invoke(instance, row.get(columnName, parameterType));
            }

            return instance;
        } catch (Exception e) {
            throw new RuntimeException("Error mapping row to entity: " + type.getName(), e);
        }
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

    private String escapeIdentifier(@NotNull String identifier) {
        return "`" + identifier + "`";
    }
}

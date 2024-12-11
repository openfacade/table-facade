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

package io.github.openfacade.table.spring.core;

import io.github.openfacade.table.api.anno.Column;
import io.github.openfacade.table.api.anno.Table;
import io.github.openfacade.table.reactive.api.ReactiveTableOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ReactiveBaseTableOperations implements ReactiveTableOperations {
    protected final Map<Class<?>, TableMetadata> classMap = new ConcurrentHashMap<>();

    @Override
    public <T> Mono<T> insert(T object) {
        Class<?> type = object.getClass();
        classMap.putIfAbsent(type, parseClass(type));
        TableMetadata metadata = classMap.get(type);
        return insert(object, metadata);
    }

    @Override
    public <T> Flux<T> findAll(Class<T> type) {
        classMap.putIfAbsent(type, parseClass(type));
        TableMetadata metadata = classMap.get(type);
        return findAll(type, metadata);
    }

    @Override
    public <T> Mono<Long> deleteAll(Class<T> type) {
        classMap.putIfAbsent(type, parseClass(type));
        TableMetadata metadata = classMap.get(type);
        return deleteAll(type, metadata);
    }

    public TableMetadata parseClass(Class<?> type) {
        if (!type.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("Class " + type.getName() + " is missing @Table annotation");
        }

        String tableName = type.getAnnotation(Table.class).name();

        LinkedHashMap<String, Method> setterMap = new LinkedHashMap<>();
        LinkedHashMap<String, Method> getterMap = new LinkedHashMap<>();

        for (Field field : type.getDeclaredFields()) {
            if (field.isAnnotationPresent(Column.class)) {
                String columnName = field.getAnnotation(Column.class).name();

                getterMap.put(columnName, getGetMethod(type, field));
                setterMap.put(columnName, getSetMethod(type, field));
            }
        }

        return new TableMetadata(tableName, setterMap, getterMap);
    }

    public static <T> Method getSetMethod(Class<T> tClass, Field classField) {
        try {
            return tClass.getMethod("set" + capitalizeFirstChar(classField.getName()), classField.getType());
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("No set method", e);
        }
    }

    public static <T> Method getGetMethod(Class<T> tClass, Field classField) {
        try {
            if (isTypeBoolean(classField.getType().getName())) {
                return tClass.getMethod("is" + capitalizeFirstChar(classField.getName()), classField.getType());
            }
            return tClass.getMethod("get" + capitalizeFirstChar(classField.getName()));
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("No get method", e);
        }
    }

    private static boolean isTypeBoolean(String typeName) {
        return "java.lang.Boolean".equals(typeName) || "boolean".equals(typeName);
    }

    private static String capitalizeFirstChar(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase(Locale.US) + str.substring(1);
    }

    public abstract <T> Mono<T> insert(T object, TableMetadata metadata);

    public abstract <T> Flux<T> findAll(Class<T> type, TableMetadata metadata);

    public abstract <T> Mono<Long> deleteAll(Class<T> type, TableMetadata metadata);
}

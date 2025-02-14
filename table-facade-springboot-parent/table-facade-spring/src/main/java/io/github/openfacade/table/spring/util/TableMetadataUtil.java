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

package io.github.openfacade.table.spring.util;

import io.github.openfacade.table.api.anno.Column;
import io.github.openfacade.table.api.anno.Table;
import io.github.openfacade.table.spring.core.TableMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Locale;

public class TableMetadataUtil {
    public static TableMetadata parseClass(@NotNull Class<?> type) {
        String tableName = getTableName(type);
        if (tableName == null) {
            throw new IllegalArgumentException("Class " + type.getName() + " is missing @Table annotation");
        }

        LinkedHashMap<String, Method> setterMap = new LinkedHashMap<>();
        LinkedHashMap<String, Method> getterMap = new LinkedHashMap<>();

        for (Field field : type.getDeclaredFields()) {
            String columnName = getColumnName(field);
            if (columnName != null) {
                getterMap.put(columnName, getGetMethod(type, field));
                setterMap.put(columnName, getSetMethod(type, field));
            }
        }

        return new TableMetadata(tableName, setterMap, getterMap);
    }

    public static String getTableName(Class<?> type) {
        if (type.isAnnotationPresent(Table.class)) {
            return type.getAnnotation(Table.class).name();
        } else if (type.isAnnotationPresent(org.springframework.data.relational.core.mapping.Table.class)) {
            org.springframework.data.relational.core.mapping.Table annotation = type.getAnnotation(org.springframework.data.relational.core.mapping.Table.class);
            if (!annotation.value().isEmpty()) {
                return annotation.value();
            }
            return annotation.name();
        }
        return null;
    }

    public static String getColumnName(Field field) {
        if (field.isAnnotationPresent(Column.class)) {
            return field.getAnnotation(Column.class).name();
        } else if (field.isAnnotationPresent(org.springframework.data.relational.core.mapping.Column.class)) {
            return field.getAnnotation(org.springframework.data.relational.core.mapping.Column.class).value();
        }
        return null;
    }

    public static <T> Method getSetMethod(Class<T> tClass, Field classField) {
        try {
            return tClass.getMethod("set" + capitalizeFirstChar(classField.getName()), classField.getType());
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("No set method", e);
        }
    }

    public static <T> Method getGetMethod(Class<T> tClass, Field classField) {
        String fieldName = classField.getName();
        String capFieldName = capitalizeFirstChar(fieldName);
        try {
            if (isTypeBoolean(classField.getType().getName())) {
                return tClass.getMethod("is" + capFieldName);
            }
            return tClass.getMethod("get" + capFieldName);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(String.format("No get method for field %s", fieldName), e);
        }
    }

    private static boolean isTypeBoolean(@NotNull String typeName) {
        return "java.lang.Boolean".equals(typeName) || "boolean".equals(typeName);
    }

    private static String capitalizeFirstChar(@Nullable String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase(Locale.US) + str.substring(1);
    }

}

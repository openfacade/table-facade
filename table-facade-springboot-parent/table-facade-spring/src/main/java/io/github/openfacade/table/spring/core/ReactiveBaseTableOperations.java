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

import io.github.openfacade.table.api.Condition;
import io.github.openfacade.table.reactive.api.ReactiveTableOperations;
import io.github.openfacade.table.spring.util.TableMetadataUtil;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ReactiveBaseTableOperations implements ReactiveTableOperations {
    protected final Map<Class<?>, TableMetadata> classMap = new ConcurrentHashMap<>();

    @Override
    public <T> Mono<T> insert(T object) {
        Class<?> type = object.getClass();
        classMap.putIfAbsent(type, TableMetadataUtil.parseClass(type));
        TableMetadata metadata = classMap.get(type);
        return insert(object, metadata);
    }

    @Override
    public <T> Mono<Long> update(Condition condition, Object[] pairs, Class<T> type) {
        classMap.putIfAbsent(type, TableMetadataUtil.parseClass(type));
        TableMetadata metadata = classMap.get(type);
        return update(condition, pairs, type, metadata);
    }

    @Override
    public <T> Mono<T> find(Condition condition, Class<T> type) {
        classMap.putIfAbsent(type, TableMetadataUtil.parseClass(type));
        TableMetadata metadata = classMap.get(type);
        return find(condition, type, metadata);
    }

    @Override
    public <T> Flux<T> findAll(Class<T> type) {
        classMap.putIfAbsent(type, TableMetadataUtil.parseClass(type));
        TableMetadata metadata = classMap.get(type);
        return findAll(type, metadata);
    }

    @Override
    public <T> Flux<T> findAll(Condition condition, Class<T> type) {
        classMap.putIfAbsent(type, TableMetadataUtil.parseClass(type));
        TableMetadata metadata = classMap.get(type);
        return findAll(condition, type, metadata);
    }

    @Override
    public <T> Mono<Long> delete(Condition condition, Class<T> type) {
        classMap.putIfAbsent(type, TableMetadataUtil.parseClass(type));
        TableMetadata metadata = classMap.get(type);
        return delete(condition, type, metadata);
    }

    @Override
    public <T> Mono<Long> deleteAll(Class<T> type) {
        classMap.putIfAbsent(type, TableMetadataUtil.parseClass(type));
        TableMetadata metadata = classMap.get(type);
        return deleteAll(type, metadata);
    }

    public abstract <T> Mono<T> insert(T object, TableMetadata metadata);

    public abstract <T> Mono<Long> update(Condition condition, Object[] pairs, Class<T> type, TableMetadata metadata);

    public abstract <T> Mono<T> find(Condition condition, Class<T> type, TableMetadata metadata);

    public abstract <T> Flux<T> findAll(Class<T> type, TableMetadata metadata);

    public abstract <T> Flux<T> findAll(Condition condition, Class<T> type, TableMetadata metadata);

    public abstract <T> Mono<Long> delete(Condition condition, Class<T> type, TableMetadata metadata);

    public abstract <T> Mono<Long> deleteAll(Class<T> type, TableMetadata metadata);
}

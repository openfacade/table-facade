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

package io.github.openfacade.table.spring.reactive.mongo;

import io.github.openfacade.table.api.Condition;
import io.github.openfacade.table.spring.core.ReactiveBaseTableOperations;
import io.github.openfacade.table.spring.core.TableMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactiveMongoTableOperations extends ReactiveBaseTableOperations {
    @Override
    public <T> Mono<T> insert(T object, TableMetadata metadata) {
        return null;
    }

    @Override
    public <T> Mono<Long> update(Condition condition, Object[] pairs, Class<T> type, TableMetadata metadata) {
        return null;
    }

    @Override
    public <T> Mono<T> find(Condition condition, Class<T> type, TableMetadata metadata) {
        return null;
    }

    @Override
    public <T> Flux<T> findAll(Class<T> type, TableMetadata metadata) {
        return null;
    }

    @Override
    public <T> Flux<T> findAll(Condition condition, Class<T> type, TableMetadata metadata) {
        return null;
    }

    @Override
    public <T> Mono<Long> delete(Condition condition, Class<T> type, TableMetadata metadata) {
        return null;
    }

    @Override
    public <T> Mono<Long> deleteAll(Class<T> type, TableMetadata metadata) {
        return null;
    }
}

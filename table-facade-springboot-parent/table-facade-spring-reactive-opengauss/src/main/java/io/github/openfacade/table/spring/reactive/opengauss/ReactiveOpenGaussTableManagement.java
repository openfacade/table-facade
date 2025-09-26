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

import io.github.openfacade.table.reactive.api.ReactiveTableManagement;
import io.github.openfacade.table.sql.mysql.MysqlSqlUtil;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class ReactiveOpenGaussTableManagement implements ReactiveTableManagement {

    private final DatabaseClient databaseClient;

    @Override
    public Mono<Boolean> existsTable(@NotNull String tableName) {
        String sql = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = ?)";
        return databaseClient.sql(sql).bind(0, tableName).map(row -> row.get(0, Boolean.class)).one();
    }

    @Override
    public Mono<Void> dropTable(@NotNull String tableName) {
        return databaseClient.sql(MysqlSqlUtil.dropTable(tableName)).fetch().rowsUpdated().then();
    }
}

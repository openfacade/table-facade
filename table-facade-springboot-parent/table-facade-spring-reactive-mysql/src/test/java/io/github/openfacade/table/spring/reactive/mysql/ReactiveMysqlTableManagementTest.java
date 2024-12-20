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

import io.github.openfacade.table.reactive.api.ReactiveTableManagement;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.test.StepVerifier;

@Slf4j
@SpringBootTest(classes = ReactiveMysqlTestConfig.class)
public class ReactiveMysqlTableManagementTest {
    @Autowired
    private DatabaseClient databaseClient;

    @Autowired
    private ReactiveTableManagement tableManagement;

    @Test
    public void testExistsTable() {
        databaseClient.sql("CREATE TABLE IF NOT EXISTS testtable (id INT PRIMARY KEY)").fetch().rowsUpdated().block();

        tableManagement.existsTable("testtable").as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();

        tableManagement.dropTable("testtable").block();

        tableManagement.existsTable("testtable").as(StepVerifier::create)
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    public void testExistsTableNotExist() {
        tableManagement.existsTable("notexisttable").as(StepVerifier::create)
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    public void testDropTableSuccess() {
        databaseClient.sql("CREATE TABLE IF NOT EXISTS testtable (id INT PRIMARY KEY)").fetch().rowsUpdated().block();
        tableManagement.dropTable("testtable").block();
    }

    @Test
    public void testDropNotExistTableFail() {
        tableManagement.dropTable("notexisttable").as(StepVerifier::create)
                .expectError()
                .verify();
    }
}

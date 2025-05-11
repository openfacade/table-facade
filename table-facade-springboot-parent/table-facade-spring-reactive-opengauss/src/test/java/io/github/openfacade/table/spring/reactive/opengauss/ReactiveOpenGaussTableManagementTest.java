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
import io.github.openfacade.table.test.common.container.OpenGaussContainer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.condition.OS.LINUX;

@Slf4j
@SpringBootTest(classes = ReactiveOpenGaussTestConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledOnOs(LINUX)
public class ReactiveOpenGaussTableManagementTest {

    private static OpenGaussContainer container;

    @Autowired
    private DatabaseClient databaseClient;

    @Autowired
    private ReactiveTableManagement tableManagement;

    @AfterAll
    void stopContainer() {
        container.stopContainer();
    }

    @BeforeEach
    void cleanTestTable() {
        // Try to drop the table if it exists (ignoring errors)
        databaseClient.sql("DROP TABLE IF EXISTS test_table").fetch().rowsUpdated().block();
    }

    @Test
    void testExistsTable() {
        databaseClient.sql("CREATE TABLE test_table (id INT PRIMARY KEY)").fetch().rowsUpdated().block();

        tableManagement.existsTable("test_table").as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();

        tableManagement.dropTable("test_table").block();

        tableManagement.existsTable("test_table").as(StepVerifier::create)
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    void testExistsTableNotExist() {
        tableManagement.existsTable("non_existent_table").as(StepVerifier::create)
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    void testDropTableSuccess() {
        databaseClient.sql("CREATE TABLE test_table (id INT PRIMARY KEY)").fetch().rowsUpdated().block();

        tableManagement.dropTable("test_table").block();

        databaseClient.sql("SELECT * FROM test_table").fetch().rowsUpdated()
                .as(StepVerifier::create)
                .expectErrorMatches(throwable -> throwable.getMessage().contains("does not exist"))
                .verify();
    }

    @Test
    void testDropNotExistTableFail() {
        tableManagement.dropTable("non_existent_table").as(StepVerifier::create)
                .expectErrorMatches(error -> error.getMessage().toLowerCase().contains("drop table failed"))
                .verify();
    }
}

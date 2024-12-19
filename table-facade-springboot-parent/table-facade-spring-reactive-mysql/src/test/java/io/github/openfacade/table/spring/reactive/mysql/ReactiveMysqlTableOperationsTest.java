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

import io.github.openfacade.table.api.ComparisonCondition;
import io.github.openfacade.table.api.ComparisonOperator;
import io.github.openfacade.table.reactive.api.ReactiveTableOperations;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(classes = ReactiveMysqlTestConfig.class)
public class ReactiveMysqlTableOperationsTest {

    @Autowired
    private DatabaseClient databaseClient;

    @Autowired
    private ReactiveTableOperations reactiveTableOperations;

    @BeforeAll
    void beforeAll() {
        String createTableSql = """
                CREATE TABLE IF NOT EXISTS test_entity (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    tinyint_boolean_field TINYINT(1),
                    blob_bytes_field BLOB,
                    varchar_string_field VARCHAR(255)
                );
                """;
        databaseClient.sql(createTableSql).fetch()
                .rowsUpdated()
                .doOnSuccess(count -> log.info("table created successfully."))
                .doOnError(error -> log.error("error creating table", error))
                .block();
    }

    @AfterAll
    void afterAll() {
        String dropTableSql = "DROP TABLE test_entity;";
        databaseClient.sql(dropTableSql).fetch()
                .rowsUpdated()
                .doOnSuccess(count -> log.info("table dropped successfully."))
                .doOnError(error -> log.error("error dropping table", error))
                .block();
    }

    @Test
    void testInsertSuccess() {
        TestMysqlEntity entityToInsert = new TestMysqlEntity();
        entityToInsert.setId(2L);
        entityToInsert.setTinyintBooleanField(true);
        entityToInsert.setBlobBytesField("Sample Data".getBytes(StandardCharsets.UTF_8));
        entityToInsert.setVarcharStringField("Sample");

        reactiveTableOperations.insert(entityToInsert)
                .doOnSuccess(insertedEntity -> log.info("Inserted entity: {}", insertedEntity))
                .block();

        Flux<TestMysqlEntity> findAllResult = reactiveTableOperations.findAll(TestMysqlEntity.class);

        List<TestMysqlEntity> entities = findAllResult
                .doOnNext(entity -> log.info("Retrieved entity: {}", entity))
                .collectList()
                .block();

        Assertions.assertNotNull(entities, "Retrieved entities should not be null");
        Assertions.assertFalse(entities.isEmpty(), "Retrieved entities should not be empty");

        TestMysqlEntity retrievedEntity = entities.get(0);
        Assertions.assertNotNull(retrievedEntity.getId(), "ID should not be null after insertion");
        Assertions.assertTrue(retrievedEntity.isTinyintBooleanField());
        Assertions.assertArrayEquals("Sample Data".getBytes(StandardCharsets.UTF_8), retrievedEntity.getBlobBytesField(), "Blob data should match");
        Assertions.assertEquals("Sample", retrievedEntity.getVarcharStringField());

        reactiveTableOperations.deleteAll(TestMysqlEntity.class)
                .doOnSuccess(deletedCount -> log.info("Deleted {} entities", deletedCount))
                .block();

        reactiveTableOperations.findAll(TestMysqlEntity.class)
                .as(StepVerifier::create)
                .expectNextCount(0)
                .verifyComplete();
    }

    @Test
    void testInsertNoIdSuccess() {
        TestMysqlEntity entityToInsert = new TestMysqlEntity();
        entityToInsert.setBlobBytesField("Sample Data".getBytes(StandardCharsets.UTF_8));

        reactiveTableOperations.insert(entityToInsert)
                .doOnSuccess(insertedEntity -> log.info("Inserted entity: {}", insertedEntity))
                .block();

        Flux<TestMysqlEntity> findAllResult = reactiveTableOperations.findAll(TestMysqlEntity.class);

        List<TestMysqlEntity> entities = findAllResult
                .doOnNext(entity -> log.info("Retrieved entity: {}", entity))
                .collectList()
                .block();

        Assertions.assertNotNull(entities, "Retrieved entities should not be null");
        Assertions.assertFalse(entities.isEmpty(), "Retrieved entities should not be empty");

        TestMysqlEntity retrievedEntity = entities.get(0);
        Assertions.assertNotNull(retrievedEntity.getId(), "ID should not be null after insertion");
        Assertions.assertArrayEquals("Sample Data".getBytes(StandardCharsets.UTF_8), retrievedEntity.getBlobBytesField(), "Blob data should match");

        reactiveTableOperations.deleteAll(TestMysqlEntity.class)
                .doOnSuccess(deletedCount -> log.info("Deleted {} entities", deletedCount))
                .block();

        reactiveTableOperations.findAll(TestMysqlEntity.class)
                .as(StepVerifier::create)
                .expectNextCount(0)
                .verifyComplete();
    }


    @Test
    void testFindByComparisonCondition() {
        TestMysqlEntity entityToInsert = new TestMysqlEntity();
        entityToInsert.setId(2L);
        entityToInsert.setTinyintBooleanField(true);
        entityToInsert.setBlobBytesField("Sample Data".getBytes(StandardCharsets.UTF_8));
        entityToInsert.setVarcharStringField("Sample");

        reactiveTableOperations.insert(entityToInsert)
                .doOnSuccess(insertedEntity -> log.info("Inserted entity: {}", insertedEntity))
                .block();

        ComparisonCondition condition = new ComparisonCondition("id", ComparisonOperator.EQ, 2L);
        TestMysqlEntity retrievedEntity = reactiveTableOperations.find(condition, TestMysqlEntity.class).block();

        Assertions.assertNotNull(retrievedEntity, "Retrieved entity should not be null");
        Assertions.assertEquals(2L, retrievedEntity.getId(), "Retrieved entity ID should match");

        Object[] pairs = {
                "tinyint_boolean_field",
                false,
                "blob_bytes_field",
                "Updated Data".getBytes(StandardCharsets.UTF_8),
                "varchar_string_field",
                "Updated Data",
        };
        reactiveTableOperations.update(condition, pairs, TestMysqlEntity.class)
                .doOnSuccess(updatedCount -> log.info("Updated {} entities", updatedCount))
                .block();

        reactiveTableOperations.find(condition, TestMysqlEntity.class)
                .as(StepVerifier::create)
                .assertNext(entity -> {
                    Assertions.assertFalse(entity.isTinyintBooleanField());
                    Assertions.assertArrayEquals("Updated Data".getBytes(StandardCharsets.UTF_8), entity.getBlobBytesField(), "Blob data should match after update");
                    Assertions.assertEquals("Updated Data", entity.getVarcharStringField());
                })
                .verifyComplete();

        reactiveTableOperations.delete(condition, TestMysqlEntity.class)
                .doOnSuccess(deletedCount -> log.info("Deleted {} entities", deletedCount))
                .block();

        reactiveTableOperations.findAll(TestMysqlEntity.class)
                .as(StepVerifier::create)
                .expectNextCount(0)
                .verifyComplete();
    }
}

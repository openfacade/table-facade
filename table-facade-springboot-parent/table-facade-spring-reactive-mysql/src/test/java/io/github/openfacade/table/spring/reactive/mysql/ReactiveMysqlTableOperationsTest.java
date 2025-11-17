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
import io.github.openfacade.table.api.CompositeCondition;
import io.github.openfacade.table.api.LogicalOperator;
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
        Assertions.assertArrayEquals("Sample Data".getBytes(StandardCharsets.UTF_8), retrievedEntity.getBlobBytesField(), "Blob data " +
                "should match");
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
        Assertions.assertArrayEquals("Sample Data".getBytes(StandardCharsets.UTF_8), retrievedEntity.getBlobBytesField(), "Blob data " +
                "should match");

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
                    Assertions.assertArrayEquals("Updated Data".getBytes(StandardCharsets.UTF_8), entity.getBlobBytesField(), "Blob data " +
                            "should match after update");
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

    @Test
    void testFindByCompositeCondition() {
        TestMysqlEntity entityToInsert = new TestMysqlEntity();
        entityToInsert.setId(3L);
        entityToInsert.setTinyintBooleanField(false);
        entityToInsert.setBlobBytesField("Composite Test Data".getBytes(StandardCharsets.UTF_8));
        entityToInsert.setVarcharStringField("CompositeTest");

        reactiveTableOperations.insert(entityToInsert)
                .doOnSuccess(insertedEntity -> log.info("Inserted entity: {}", insertedEntity))
                .block();

        // Create a composite condition: id = 3 AND varchar_string_field = "CompositeTest"
        ComparisonCondition idCondition = new ComparisonCondition("id", ComparisonOperator.EQ, 3L);
        ComparisonCondition varcharCondition = new ComparisonCondition("varchar_string_field", ComparisonOperator.EQ, "CompositeTest");

        CompositeCondition compositeCondition = CompositeCondition.builder()
                .operator(LogicalOperator.AND)
                .condition(idCondition)
                .condition(varcharCondition)
                .build();

        TestMysqlEntity retrievedEntity = reactiveTableOperations.find(compositeCondition, TestMysqlEntity.class).block();

        Assertions.assertNotNull(retrievedEntity, "Retrieved entity should not be null");
        Assertions.assertEquals(3L, retrievedEntity.getId(), "Retrieved entity ID should match");
        Assertions.assertFalse(retrievedEntity.isTinyintBooleanField());
        Assertions.assertArrayEquals("Composite Test Data".getBytes(StandardCharsets.UTF_8), retrievedEntity.getBlobBytesField(), "Blob " +
                "data should match");
        Assertions.assertEquals("CompositeTest", retrievedEntity.getVarcharStringField());

        // Test with OR condition: id = 3 OR varchar_string_field = "NonExistent"
        CompositeCondition orCondition = CompositeCondition.builder()
                .operator(LogicalOperator.OR)
                .condition(idCondition)
                .condition(new ComparisonCondition("varchar_string_field", ComparisonOperator.EQ, "NonExistent"))
                .build();

        TestMysqlEntity orRetrievedEntity = reactiveTableOperations.find(orCondition, TestMysqlEntity.class).block();

        Assertions.assertNotNull(orRetrievedEntity, "Retrieved entity should not be null for OR condition");
        Assertions.assertEquals(3L, orRetrievedEntity.getId(), "Retrieved entity ID should match for OR condition");

        reactiveTableOperations.delete(compositeCondition, TestMysqlEntity.class)
                .doOnSuccess(deletedCount -> log.info("Deleted {} entities", deletedCount))
                .block();

        reactiveTableOperations.findAll(TestMysqlEntity.class)
                .as(StepVerifier::create)
                .expectNextCount(0)
                .verifyComplete();
    }

    @Test
    void testFindByComplexCompositeCondition() {
        // Insert test data
        TestMysqlEntity entity1 = new TestMysqlEntity();
        entity1.setId(4L);
        entity1.setTinyintBooleanField(true);
        entity1.setBlobBytesField("Complex Test Data 1".getBytes(StandardCharsets.UTF_8));
        entity1.setVarcharStringField("ComplexTest1");

        TestMysqlEntity entity2 = new TestMysqlEntity();
        entity2.setId(5L);
        entity2.setTinyintBooleanField(false);
        entity2.setBlobBytesField("Complex Test Data 2".getBytes(StandardCharsets.UTF_8));
        entity2.setVarcharStringField("ComplexTest2");

        reactiveTableOperations.insert(entity1)
                .doOnSuccess(insertedEntity -> log.info("Inserted entity1: {}", insertedEntity))
                .block();

        reactiveTableOperations.insert(entity2)
                .doOnSuccess(insertedEntity -> log.info("Inserted entity2: {}", insertedEntity))
                .block();

        // Create a complex composite condition: (id = 4 AND varchar_string_field = "ComplexTest1") OR (id = 5 AND tinyint_boolean_field
        // = true)
        // This should return entity1 because the first part of OR condition matches
        ComparisonCondition idCondition1 = new ComparisonCondition("id", ComparisonOperator.EQ, 4L);
        ComparisonCondition varcharCondition1 = new ComparisonCondition("varchar_string_field", ComparisonOperator.EQ, "ComplexTest1");
        CompositeCondition andCondition1 = CompositeCondition.builder()
                .operator(LogicalOperator.AND)
                .condition(idCondition1)
                .condition(varcharCondition1)
                .build();

        ComparisonCondition idCondition2 = new ComparisonCondition("id", ComparisonOperator.EQ, 5L);
        ComparisonCondition booleanCondition = new ComparisonCondition("tinyint_boolean_field", ComparisonOperator.EQ, true);
        CompositeCondition andCondition2 = CompositeCondition.builder()
                .operator(LogicalOperator.AND)
                .condition(idCondition2)
                .condition(booleanCondition)
                .build();

        CompositeCondition complexCondition = CompositeCondition.builder()
                .operator(LogicalOperator.OR)
                .condition(andCondition1)
                .condition(andCondition2)
                .build();

        TestMysqlEntity retrievedEntity = reactiveTableOperations.find(complexCondition, TestMysqlEntity.class).block();

        // Should find entity1 because (id = 4 AND varchar_string_field = "ComplexTest1") is true
        Assertions.assertNotNull(retrievedEntity, "Retrieved entity should not be null");
        Assertions.assertEquals(4L, retrievedEntity.getId(), "Retrieved entity ID should match");
        Assertions.assertTrue(retrievedEntity.isTinyintBooleanField());
        Assertions.assertArrayEquals("Complex Test Data 1".getBytes(StandardCharsets.UTF_8), retrievedEntity.getBlobBytesField(), "Blob " +
                "data should match");
        Assertions.assertEquals("ComplexTest1", retrievedEntity.getVarcharStringField());

        // Create another complex composite condition: (id = 4 AND tinyint_boolean_field = false) OR (id = 5 AND tinyint_boolean_field =
        // true)
        // This should return null because neither part of OR condition is fully satisfied
        ComparisonCondition booleanConditionFalse = new ComparisonCondition("tinyint_boolean_field", ComparisonOperator.EQ, false);
        CompositeCondition andCondition1False = CompositeCondition.builder()
                .operator(LogicalOperator.AND)
                .condition(idCondition1)
                .condition(booleanConditionFalse)
                .build();

        CompositeCondition complexConditionNoResult = CompositeCondition.builder()
                .operator(LogicalOperator.OR)
                .condition(andCondition1False)
                .condition(andCondition2)
                .build();

        TestMysqlEntity noResultEntity = reactiveTableOperations.find(complexConditionNoResult, TestMysqlEntity.class).block();

        // Should not find any entity because neither (id = 4 AND tinyint_boolean_field = false) nor (id = 5 AND tinyint_boolean_field =
        // true) is fully satisfied
        Assertions.assertNull(noResultEntity, "Should not find any entity for this condition");

        // Clean up test data
        reactiveTableOperations.deleteAll(TestMysqlEntity.class)
                .doOnSuccess(deletedCount -> log.info("Deleted {} entities", deletedCount))
                .block();

        reactiveTableOperations.findAll(TestMysqlEntity.class)
                .as(StepVerifier::create)
                .expectNextCount(0)
                .verifyComplete();
    }

    @Test
    void testFindAllSuccess() {
        // insert 3 records for testing
        for (int idx = 0; idx < 3; idx++) {
            long id = idx + 5;
            TestMysqlEntity entityToInsert = new TestMysqlEntity();
            entityToInsert.setId(id);
            entityToInsert.setTinyintBooleanField(true);
            entityToInsert.setBlobBytesField("Sample Data".getBytes(StandardCharsets.UTF_8));
            String str = id % 2 == 0 ? "Even" : "Odd";
            entityToInsert.setVarcharStringField(str);

            reactiveTableOperations.insert(entityToInsert)
                    .doOnSuccess(insertedEntity -> log.info("Inserted entity: {}", insertedEntity))
                    .block();
        }

        // query condition
        ComparisonCondition idCondition = new ComparisonCondition("id", ComparisonOperator.GTE, 5L);
        ComparisonCondition varcharCondition = new ComparisonCondition("varchar_string_field", ComparisonOperator.EQ, "Odd");
        CompositeCondition andCondition = CompositeCondition.builder()
                .operator(LogicalOperator.AND)
                .condition(idCondition)
                .condition(varcharCondition)
                .build();

        Flux<TestMysqlEntity> findAllResult = reactiveTableOperations.findAll(andCondition, TestMysqlEntity.class);

        List<TestMysqlEntity> entities = findAllResult
                .doOnNext(entity -> log.info("Retrieved entity: {}", entity))
                .collectList()
                .block();

        Assertions.assertNotNull(entities);
        Assertions.assertEquals(2, entities.size());
        Assertions.assertEquals("Odd", entities.get(0).getVarcharStringField());
        Assertions.assertEquals("Odd", entities.get(1).getVarcharStringField());

        reactiveTableOperations.deleteAll(TestMysqlEntity.class)
                .doOnSuccess(deletedCount -> log.info("Deleted {} entities", deletedCount))
                .block();
    }
}

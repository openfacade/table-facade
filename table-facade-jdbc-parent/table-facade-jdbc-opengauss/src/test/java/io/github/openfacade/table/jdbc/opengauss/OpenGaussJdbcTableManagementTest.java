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

package io.github.openfacade.table.jdbc.opengauss;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.openfacade.table.api.OpenGaussConfig;
import io.github.openfacade.table.api.TableException;
import io.github.openfacade.table.test.common.container.OpenGaussContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;

import java.sql.Connection;
import java.sql.Statement;
import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.condition.OS.LINUX;

class OpenGaussJdbcTableManagementTest {

    private static DataSource dataSource;

    private OpenGaussJdbcTableManagement tableManagement;
    private static String jdbcUrl = "jdbc:postgresql://localhost:5432/%s?currentSchema=%s";
    private static String openGaussDriver = "org.postgresql.Driver";

    private static OpenGaussContainer container;

    @BeforeAll
    static void setUp() {
        if (!System.getProperty("os.name").toLowerCase().contains("linux")) {
            return;
        }
        container = new OpenGaussContainer().withCompatibility("B");
        container.startContainer();
        String openGaussJdbcUrl = String.format(jdbcUrl, container.getDatabaseName(), container.getSchema());
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(openGaussJdbcUrl);
        config.setUsername(container.getUsername());
        config.setPassword(container.getPassword());
        config.setDriverClassName(openGaussDriver);
        dataSource = new HikariDataSource(config);
    }

    @AfterAll
    static void destroy() {
        if (!System.getProperty("os.name").toLowerCase().contains("linux")) {
            return;
        }
        container.stopContainer();
    }

    @BeforeEach
    void init() {
        OpenGaussConfig openGaussConfig = new OpenGaussConfig() {{
            setSchema(container.getSchema());
        }};
        tableManagement = new OpenGaussJdbcTableManagement(dataSource, openGaussConfig);
    }

    @Test
    @EnabledOnOs(LINUX)
    void testDropTableSuccess() throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS test_table (id INT PRIMARY KEY)");
        }

        tableManagement.dropTable("test_table");

        try (Connection connection = dataSource.getConnection();
             Statement stmt = connection.createStatement()) {
            stmt.executeQuery("SELECT * FROM test_table");
            fail("relation \"test_table\" does not exist on sgnode");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("does not exist"));
        }
    }

    @Test
    @EnabledOnOs(LINUX)
    public void testDropNotExistTableFail() {
        Exception exception = assertThrows(TableException.class, () -> {
            tableManagement.dropTable("non_existent_table");
        });

        String expectedMessage = "drop table failed";
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains(expectedMessage));
    }
}

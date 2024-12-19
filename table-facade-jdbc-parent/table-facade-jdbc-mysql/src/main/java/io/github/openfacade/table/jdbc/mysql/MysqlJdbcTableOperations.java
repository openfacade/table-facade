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

package io.github.openfacade.table.jdbc.mysql;

import io.github.openfacade.table.api.TableException;
import io.github.openfacade.table.api.TableOperations;
import io.github.openfacade.table.sql.mysql.MysqlSqlUtil;
import lombok.RequiredArgsConstructor;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

@RequiredArgsConstructor
public class MysqlJdbcTableOperations implements TableOperations {
    private final DataSource dataSource;

    @Override
    public <T> Long deleteAll(Class<T> type) throws TableException {
        io.github.openfacade.table.api.anno.Table tableAnnotation = type.getAnnotation(io.github.openfacade.table.api.anno.Table.class);
        if (tableAnnotation == null || tableAnnotation.name().isEmpty()) {
            throw new TableException("Class " + type.getName() + " does not have a Table annotation with a valid name.");
        }

        String tableName = tableAnnotation.name();
        String sql = MysqlSqlUtil.deleteAll(tableName);

        try (Connection connection = dataSource.getConnection();
             Statement stmt = connection.createStatement()) {
            int rowsAffected = stmt.executeUpdate(sql);
            return (long) rowsAffected;
        } catch (SQLException e) {
            throw new TableException("Failed to delete all records from table " + tableName, e);
        }
    }
}

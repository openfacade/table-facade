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

import io.github.openfacade.table.api.Table;
import io.github.openfacade.table.api.TableException;
import io.github.openfacade.table.api.TableManagement;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

@RequiredArgsConstructor
public class MysqlJdbcTableManagement implements TableManagement {
    private final DataSource dataSource;

    @Override
    public List<Table> showTables() throws TableException {
        try (Connection connection = dataSource.getConnection();
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW TABLES")) {
            List<Table> tables = new ArrayList<>();
            while (rs.next()) {
                Table table = new Table();
                table.setName(rs.getString(1));
                tables.add(table);
            }
            return tables;
        } catch (SQLException e) {
            throw new TableException("show tables failed", e);
        }
    }

    @Override
    public void dropTable(@NotNull String tableName) throws TableException {
        String sql = "DROP TABLE `" + tableName + '`';
        try (Connection connection = dataSource.getConnection();
             Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            throw new TableException("drop table failed " + tableName, e);
        }
    }
}

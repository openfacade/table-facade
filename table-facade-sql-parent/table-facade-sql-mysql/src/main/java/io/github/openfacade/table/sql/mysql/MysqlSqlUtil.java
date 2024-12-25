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

package io.github.openfacade.table.sql.mysql;

import io.github.openfacade.table.sql.common.SqlBuilder;

public class MysqlSqlUtil {
    public static String dropTable(String tableName) {
        SqlBuilder sqlBuilder = new SqlBuilder()
                .keyword("DROP TABLE")
                .quote('`')
                .identifier(tableName);
        return sqlBuilder.build();
    }

    public static String dropTableIfExists(String tableName) {
        SqlBuilder sqlBuilder = new SqlBuilder()
                .keyword("DROP TABLE IF EXISTS")
                .quote('`')
                .identifier(tableName);
        return sqlBuilder.build();
    }

    public static String deleteAll(String tableName) {
        SqlBuilder sqlBuilder = new SqlBuilder()
                .keyword("DELETE FROM")
                .quote('`')
                .identifier(tableName);
        return sqlBuilder.build();
    }

    public static String count(String tableName) {
        SqlBuilder sqlBuilder = new SqlBuilder()
                .keyword("SELECT COUNT(*) FROM")
                .quote('`')
                .identifier(tableName);
        return sqlBuilder.build();
    }
}

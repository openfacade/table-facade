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

package io.github.openfacade.table.spring.config;

import io.github.openfacade.table.api.DriverType;
import io.github.openfacade.table.reactive.api.ReactiveTableManagement;
import io.github.openfacade.table.reactive.api.ReactiveTableOperations;
import io.github.openfacade.table.spring.core.TableFacadeProperties;
import io.github.openfacade.table.spring.reactive.mysql.ReactiveMysqlTableManagement;
import io.github.openfacade.table.spring.reactive.mysql.ReactiveMysqlTableOperations;
import io.github.openfacade.table.spring.reactive.opengauss.ReactiveOpenGaussTableManagement;
import io.github.openfacade.table.spring.reactive.opengauss.ReactiveOpenGaussTableOperations;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.r2dbc.core.DatabaseClient;

@AutoConfiguration
@EnableConfigurationProperties(TableFacadeProperties.class)
public class TableFacadeReactiveAutoConfiguration {
    private final TableFacadeProperties tableFacadeProperties;

    private final DatabaseClient databaseClient;

    public TableFacadeReactiveAutoConfiguration(TableFacadeProperties tableFacadeProperties,
                                                DatabaseClient databaseClient) {
        this.tableFacadeProperties = tableFacadeProperties;
        this.databaseClient = databaseClient;
    }

    @Bean
    @ConditionalOnMissingBean(ReactiveTableOperations.class)
    public ReactiveTableOperations reactiveTableOperations() {
        if (tableFacadeProperties.getDriverType().equals(DriverType.openGauss)) {
            return new ReactiveOpenGaussTableOperations(databaseClient);
        } else {
            return new ReactiveMysqlTableOperations(databaseClient);
        }
    }

    @Bean
    @ConditionalOnMissingBean(ReactiveTableManagement.class)
    public ReactiveTableManagement reactiveTableManagement() {
        if (tableFacadeProperties.getDriverType().equals(DriverType.openGauss)) {
            return new ReactiveOpenGaussTableManagement(databaseClient);
        } else {
            return new ReactiveMysqlTableManagement(databaseClient);
        }
    }
}

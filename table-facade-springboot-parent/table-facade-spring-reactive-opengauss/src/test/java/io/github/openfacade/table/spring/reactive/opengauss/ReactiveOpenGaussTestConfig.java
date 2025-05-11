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

import io.github.openfacade.table.spring.test.common.TestConfig;
import io.github.openfacade.table.test.common.container.OpenGaussContainer;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.springframework.boot.autoconfigure.r2dbc.R2dbcProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.r2dbc.core.DatabaseClient;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class ReactiveOpenGaussTestConfig extends TestConfig {

    private static final OpenGaussContainer openGaussContainer = new OpenGaussContainer().withCompatibility("B");

    static {
        openGaussContainer.startContainer();
    }

    @Bean
    public DatabaseClient databaseClient(ConnectionFactory connectionFactory) {
        return DatabaseClient.create(connectionFactory);
    }

    @Bean
    public ConnectionFactory connectionFactory(R2dbcProperties r2dbcProperties) {
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
                .option(DRIVER, "postgresql")
                .option(HOST, "localhost")
                .option(PORT, 5432)
                .option(USER, openGaussContainer.getUsername())
                .option(PASSWORD, openGaussContainer.getPassword())
                .option(DATABASE, openGaussContainer.getDatabaseName())
                .build();
        return ConnectionFactories.get(options);
    }

    @Bean
    public R2dbcProperties r2dbcProperties() {
        R2dbcProperties properties = new R2dbcProperties();
        String url = String.format("r2dbc:postgresql://%s:%d/%s",
                "localhost",
                5432,
                openGaussContainer.getDatabaseName());
        properties.setUrl(url);
        properties.setUsername(openGaussContainer.getUsername());
        properties.setPassword(openGaussContainer.getPassword());
        return properties;
    }
}

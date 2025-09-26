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

package io.github.openfacade.table.test.common.container;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.PullImageResultCallback;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.api.model.PullResponseItem;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import lombok.Getter;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.github.dockerjava.api.model.HostConfig.newHostConfig;

public class OpenGaussContainer {

    @Getter
    private String databaseName;

    @Getter
    private String username;

    @Getter
    private String password;

    @Getter
    private String schema;

    @Getter
    private String compatibility;

    private final DockerClient dockerClient;

    private Optional<CreateContainerResponse> response;

    private final static String containerName = "opengauss";

    private final static String imageRepository = "ttbb/opengauss";

    private final static String imageTag = "mate";

    private final static String imageName = imageRepository + ":" + imageTag;

    private final static String defaultUserName = "test";

    private final static String defaultCompatibility = "PG";

    private final static String defaultSchemaName = "testdb";

    private final static String defaultDatabaseName = "testdb";

    private final static String defaultPassword = "Test@123";

    public OpenGaussContainer() {
        DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder().build();
        DockerHttpClient httpClient5 = new ApacheDockerHttpClient.Builder()
                .dockerHost(config.getDockerHost())
                .build();
        DockerClient dockerClient = DockerClientImpl.getInstance(config, httpClient5);
        dockerClient.pingCmd().exec();
        this.dockerClient = dockerClient;
        this.username = defaultUserName;
        this.password = defaultPassword;
        this.databaseName = defaultDatabaseName;
        this.schema = defaultSchemaName;
        this.compatibility = defaultCompatibility;
    }

    public OpenGaussContainer withDatabaseName(@NotNull String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    public OpenGaussContainer withUsername(@NotNull String username) {
        this.username = username;
        return this;
    }

    public OpenGaussContainer withPassword(@NotNull String password) {
        this.password = password;
        return this;
    }

    public OpenGaussContainer withSchema(@NotNull String schema) {
        this.schema = schema;
        return this;
    }

    public OpenGaussContainer withCompatibility(@NotNull String compatibility) {
        this.compatibility = compatibility;
        return this;
    }

    @SneakyThrows
    public void startContainer() {
        String[] envs = new String[]{"USER_NAME=" + username, "USER_PASSWORD=" + password, "PASSWORD=" + password,
                "CUSTOM_DATABASE=" + databaseName, "CUSTOM_DATABASE_DBCOMPATIBILITY=" + compatibility,
                "CUSTOM_SCHEMA=" + schema};
        ExposedPort tcp5432 = ExposedPort.tcp(5432);
        Ports portBindings = new Ports();
        portBindings.bind(tcp5432, Ports.Binding.bindPort(5432));
        dockerClient.pullImageCmd(imageRepository).withTag(imageTag).exec(new PullImageResultCallback() {
            @Override
            public void onNext(PullResponseItem item) {
                super.onNext(item);
            }
        }).awaitCompletion();
        CreateContainerResponse containerResponse = dockerClient.createContainerCmd(imageName)
                .withName(containerName)
                .withExposedPorts(tcp5432)
                .withHostConfig(newHostConfig()
                        .withPortBindings()
                        .withPortBindings(portBindings)
                        .withPrivileged(true))
                .withEnv(envs).exec();
        this.response = Optional.of(containerResponse);
        dockerClient.startContainerCmd(containerResponse.getId()).exec();
        TimeUnit.MINUTES.sleep(1);
    }

    public void stopContainer() {
        response.ifPresent(resp -> dockerClient.stopContainerCmd(resp.getId()).exec());
        response.ifPresent(resp -> dockerClient.removeContainerCmd(resp.getId()).exec());
    }

}

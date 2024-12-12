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

import io.github.openfacade.table.api.anno.Column;
import io.github.openfacade.table.api.anno.Table;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@Table(name = "test_entity")
public class TestMysqlEntity {
    @Column(name = "id")
    private Long id;

    @Column(name = "tinyint_boolean_field")
    private boolean tinyintBooleanField;

    @Column(name = "blob_bytes_field")
    private byte[] blobBytesField;

    @Column(name = "varchar_string_field")
    private String varcharStringField;
}

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

package io.github.openfacade.table.api;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class CompositeCondition implements Condition {
    private final LogicalOperator operator;
    private final List<Condition> conditions;

    private CompositeCondition(LogicalOperator operator, List<Condition> conditions) {
        this.operator = operator;
        this.conditions = conditions;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private LogicalOperator operator;
        private List<Condition> conditions = new ArrayList<>();

        public Builder operator(LogicalOperator operator) {
            this.operator = operator;
            return this;
        }

        public Builder condition(Condition condition) {
            this.conditions.add(condition);
            return this;
        }

        public Builder conditions(List<Condition> conditions) {
            this.conditions.addAll(conditions);
            return this;
        }

        public CompositeCondition build() {
            if (operator == null) {
                throw new IllegalStateException("Operator must be set");
            }
            if (conditions.isEmpty()) {
                throw new IllegalStateException("At least one condition must be added");
            }
            return new CompositeCondition(operator, conditions);
        }
    }
}

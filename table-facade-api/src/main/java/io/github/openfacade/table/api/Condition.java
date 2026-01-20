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

public interface Condition {
    
    /**
     * 创建一个等于条件
     * @param column 列名
     * @param value 值
     * @return 等于条件
     */
    static Condition eq(String column, Object value) {
        return new ComparisonCondition(column, ComparisonOperator.EQ, value);
    }
    
    /**
     * 创建一个不等于条件
     * @param column 列名
     * @param value 值
     * @return 不等于条件
     */
    static Condition neq(String column, Object value) {
        return new ComparisonCondition(column, ComparisonOperator.NEQ, value);
    }
    
    /**
     * 创建一个大于条件
     * @param column 列名
     * @param value 值
     * @return 大于条件
     */
    static Condition gt(String column, Object value) {
        return new ComparisonCondition(column, ComparisonOperator.GT, value);
    }
    
    /**
     * 创建一个小于条件
     * @param column 列名
     * @param value 值
     * @return 小于条件
     */
    static Condition lt(String column, Object value) {
        return new ComparisonCondition(column, ComparisonOperator.LT, value);
    }
    
    /**
     * 创建一个大于等于条件
     * @param column 列名
     * @param value 值
     * @return 大于等于条件
     */
    static Condition gte(String column, Object value) {
        return new ComparisonCondition(column, ComparisonOperator.GTE, value);
    }
    
    /**
     * 创建一个小于等于条件
     * @param column 列名
     * @param value 值
     * @return 小于等于条件
     */
    static Condition lte(String column, Object value) {
        return new ComparisonCondition(column, ComparisonOperator.LTE, value);
    }
}

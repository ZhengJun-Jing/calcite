/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.adapter.dm;


import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;
/** Schema factory that creates a
 * {@link org.apache.calcite.adapter.dm.DmSchema}.
 *
 * <p>This allows you to create a jdbc schema inside a model.json file, like
 * this:
 *
 * <blockquote><pre>
 * {
 *   "version": "1.0",
 *   "defaultSchema": "FOODMART_CLONE",
 *   "schemas": [
 *     {
 *       "name": "FOODMART_CLONE",
 *       "type": "custom",
 *       "factory": "org.apache.calcite.adapter.jdbc.JdbcSchema$Factory",
 *       "operand": {
 *         "jdbcDriver": "com.mysql.jdbc.Driver",
 *         "jdbcUrl": "jdbc:mysql://localhost/foodmart",
 *         "jdbcUser": "foodmart",
 *         "jdbcPassword": "foodmart"
 *       }
 *     }
 *   ]
 * }</pre></blockquote>
 */
public  class DmSchemaFactory implements SchemaFactory {
    public static final DmSchemaFactory INSTANCE = new DmSchemaFactory();

    private DmSchemaFactory() {}

    @Override public Schema create(
            SchemaPlus parentSchema,
            String name,
            Map<String, Object> operand) {
        return DmSchema.create(parentSchema, name, operand);
    }
}

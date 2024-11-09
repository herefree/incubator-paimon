/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark;

import org.apache.paimon.types.DataField;

import org.apache.spark.sql.connector.catalog.View;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

/** Spark {@link View} for paimon. */
public class SparkView implements View {

    public static final String QUERY_COLUMN_NAMES = "spark.query-column-names";

    private final String catalogName;

    private final org.apache.paimon.view.View paimonView;

    public SparkView(String catalogName, org.apache.paimon.view.View paimonView) {
        this.paimonView = paimonView;
        this.catalogName = catalogName;
    }

    @Override
    public String name() {
        return paimonView.fullName();
    }

    @Override
    public String query() {
        return paimonView.query();
    }

    @Override
    public String currentCatalog() {
        return catalogName;
    }

    @Override
    public String[] currentNamespace() {
        return new String[] {paimonView.fullName().split("\\.")[0]};
    }

    @Override
    public StructType schema() {
        return SparkTypeUtils.fromPaimonRowType(paimonView.rowType());
    }

    @Override
    public String[] queryColumnNames() {
        return paimonView.options().containsKey(QUERY_COLUMN_NAMES)
                ? paimonView.options().get(QUERY_COLUMN_NAMES).split(",")
                : new String[0];
    }

    @Override
    public String[] columnAliases() {
        return paimonView.rowType().getFields().stream()
                .map(DataField::name)
                .toArray(String[]::new);
    }

    @Override
    public String[] columnComments() {
        return paimonView.rowType().getFields().stream()
                .map(DataField::description)
                .toArray(String[]::new);
    }

    @Override
    public Map<String, String> properties() {
        return paimonView.options();
    }
}

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

package org.apache.paimon.fileindex.dynamicbloomfilter;

import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fileindex.FileIndexerFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;

/** Factory to create {@link DynamicBloomFilterFileIndexFactory}. */
public class DynamicBloomFilterFileIndexFactory implements FileIndexerFactory {

    public static final String DYNAMIC_BLOOM_FILTER = "dynamic-bloom-filter";

    @Override
    public String identifier() {
        return DYNAMIC_BLOOM_FILTER;
    }

    @Override
    public FileIndexer create(DataType dataType, Options options) {
        return new DynamicBloomFilterFileIndex(dataType, options);
    }
}

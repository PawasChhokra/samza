/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.sql.data.serializers;

import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.sql.data.string.StringData;

import java.io.UnsupportedEncodingException;

public class SqlStringSerde implements Serde<StringData> {

    private final Serde<String> serde;

    public SqlStringSerde(String encoding) {
        this.serde = new StringSerde(encoding);
    }

    @Override
    public StringData fromBytes(byte[] bytes) {
          return new StringData(serde.fromBytes(bytes));
    }

    @Override
    public byte[] toBytes(StringData object) {
        return serde.toBytes(object.strValue());
    }
}

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

package org.apache.kylin.engine.spark2.common.persistence;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;

import java.io.IOException;

public class RawResource {
    private String resPath;
    @JsonSerialize(using = ByteSourceSerializer.class)
    @JsonDeserialize(using = BytesourceDeserializer.class)
    private ByteSource byteSource;
    private long timestamp;
    private long mvcc;

    public ByteSource getByteSource() {
        return byteSource;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getMvcc() {
        return mvcc;
    }

    public String getResPath() {
        return resPath;
    }

    private static class ByteSourceSerializer extends JsonSerializer<ByteSource> {
        @Override
        public void serialize(ByteSource value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException, JsonProcessingException {
            byte[] bytes = value.read();
            gen.writeBinary(bytes);
        }
    }

    private static class BytesourceDeserializer extends JsonDeserializer<ByteSource> {
        @Override
        public ByteSource deserialize(JsonParser p, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
            byte[] bytes = p.getBinaryValue();
            return ByteStreams.asByteSource(bytes);
        }
    }
}

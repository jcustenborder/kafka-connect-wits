/**
 * Copyright © 2019 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.wits;

import com.github.jcustenborder.netty.wits.Record;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;

import java.net.SocketAddress;
import java.time.ZoneId;

abstract class AbstractRecordConverter {
  protected final Schema keySchema;

  AbstractRecordConverter() {
    SchemaBuilder keyBuilder = SchemaBuilder.struct()
        .name("com.github.jcustenborder.kafka.connect.wits.WITSKey")
        .field(
            "wellId",
            SchemaBuilder.string()
                .optional()
                .parameter("wits.field.id", "1")
                .doc("Well Identifier")
                .build()
        );
    this.keySchema = keyBuilder.build();
  }

  public SchemaAndValue convertKey(Record record) {
    if (null == record) {
      return SchemaAndValue.NULL;
    }
    Struct key = new Struct(this.keySchema)
        .put("wellId", record.wellId());
    return new SchemaAndValue(this.keySchema, key);
  }

  static final ZoneId UTC = ZoneId.of("UTC");

  public SourceRecord sourceRecord(Record record, SocketAddress remoteAddress) {
    Long timestamp = null;
    SchemaAndValue key = convertKey(record);
    SchemaAndValue value = convertValue(record);

    if (null != record.dateTime()) {
      timestamp = record.dateTime().atZone(UTC).toInstant().toEpochMilli();
    }
    ConnectHeaders headers = new ConnectHeaders();
    headers.addString("wits.host", remoteAddress.toString());
    SourceRecord sourceRecord = new SourceRecord(
        null,
        null,
        value.schema().name(),
        null,
        key.schema(),
        key.value(),
        value.schema(),
        value.value(),
        timestamp,
        headers
    );
    return sourceRecord;
  }

  protected abstract SchemaAndValue convertValue(Record record);
}

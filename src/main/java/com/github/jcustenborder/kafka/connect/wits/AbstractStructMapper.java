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
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;

abstract class AbstractStructMapper {
  protected final Schema schema;

  protected AbstractStructMapper(Schema schema) {
    this.schema = schema;
  }

  static SchemaBuilder addSchemaBase(SchemaBuilder fieldBuilder, int fieldNumber, String documentation) {
    fieldBuilder.optional();
    fieldBuilder.doc(documentation);
    fieldBuilder.parameter("wits.field.id", Integer.toString(fieldNumber));
    return fieldBuilder;
  }

  protected static void addTimeField(SchemaBuilder builder, int fieldNumber, String fieldName, String documentation) {
    SchemaBuilder fieldBuilder = addSchemaBase(Time.builder(), fieldNumber, documentation);
    builder.field(fieldName, fieldBuilder.build());
  }

  protected static void addDateField(SchemaBuilder builder, int fieldNumber, String fieldName, String documentation) {
    SchemaBuilder fieldBuilder = addSchemaBase(Date.builder(), fieldNumber, documentation);
    builder.field(fieldName, fieldBuilder.build());
  }

  protected static void addInt32Field(SchemaBuilder builder, int fieldNumber, String fieldName, String documentation) {
    SchemaBuilder fieldBuilder = addSchemaBase(SchemaBuilder.int32(), fieldNumber, documentation);
    builder.field(fieldName, fieldBuilder.build());
  }

  protected static void addInt16Field(SchemaBuilder builder, int fieldNumber, String fieldName, String documentation) {
    SchemaBuilder fieldBuilder = addSchemaBase(SchemaBuilder.int16(), fieldNumber, documentation);
    builder.field(fieldName, fieldBuilder.build());
  }

  protected static void addFloat32Field(SchemaBuilder builder, int fieldNumber, String fieldName, String documentation) {
    SchemaBuilder fieldBuilder = addSchemaBase(SchemaBuilder.float32(), fieldNumber, documentation);
    builder.field(fieldName, fieldBuilder.build());
  }

  protected static void addStringField(SchemaBuilder builder, int fieldNumber, String fieldName, String documentation) {
    SchemaBuilder fieldBuilder = addSchemaBase(SchemaBuilder.string(), fieldNumber, documentation);
    builder.field(fieldName, fieldBuilder.build());
  }

  protected static void addDateTimeField(SchemaBuilder builder) {
    SchemaBuilder fieldBuilder = Timestamp.builder()
        .optional()
        .doc("Timestamp the event occurred.");
    builder.field("dateTime", fieldBuilder.build());
  }

  public abstract Class<? extends Record> recordClass();

  public abstract SchemaAndValue convert(Record record);


  protected Object convertDate(LocalDate date) {
    if (null == date) {
      return null;
    }
    return convertDateTime(LocalDateTime.of(date, LocalTime.MIN));
  }

  protected Object convertTime(LocalTime time) {
    if (null == time) {
      return null;
    }
    return convertDateTime(time.atDate(LocalDate.ofEpochDay(0)));

  }

  protected Object convertDateTime(LocalDateTime dateTime) {
    if (null == dateTime) {
      return null;
    }
    return java.util.Date.from(
        dateTime.atZone(ZoneId.of("UTC")).toInstant()
    );
  }
}

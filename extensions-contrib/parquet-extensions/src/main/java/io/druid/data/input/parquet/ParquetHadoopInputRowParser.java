/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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
package io.druid.data.input.parquet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.input.AvroStreamInputRowParser;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.data.input.parquet.model.Field;
import io.druid.data.input.parquet.model.FieldType;
import io.druid.data.input.parquet.model.ParquetParser;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetHadoopInputRowParser implements InputRowParser<GenericRecord> {
    private final ParseSpec parseSpec;
    private final boolean binaryAsString;
    private final List<String> dimensions;
    private final String parquetParserString;
    private final ParquetParser parquetParser;
    private static final String EMPTY_STRING = "";

    @JsonCreator
    public ParquetHadoopInputRowParser(
            @JsonProperty("parseSpec") ParseSpec parseSpec,
            @JsonProperty("binaryAsString") Boolean binaryAsString,
            @JsonProperty("parquetParser") String parquetParserString
    ) {
        this.parseSpec = parseSpec;
        this.binaryAsString = binaryAsString == null ? false : binaryAsString;
        this.parquetParserString = parquetParserString;
        this.parquetParser = parquetParserString == null ? null :
                JsonUtils.readFrom(parquetParserString, ParquetParser.class);
        this.parquetParser.setParsedFields(Field.parseFields(this.parquetParser.getFields()));

        List<DimensionSchema> dimensionSchema = parseSpec.getDimensionsSpec().getDimensions();
        this.dimensions = Lists.newArrayList();
        for (DimensionSchema dim : dimensionSchema) {
            this.dimensions.add(dim.getName());
        }
    }

    /**
     * imitate avro extension {@link AvroStreamInputRowParser#parseGenericRecord(GenericRecord, ParseSpec, List, boolean, boolean)}
     */
    @Override
    public InputRow parse(GenericRecord record) {
//        GenericRecordAsMap genericRecordAsMap = new GenericRecordAsMap(record, false, binaryAsString);
        final Map<String, Object> event = getEvent(this.parquetParser.getParsedFields(), record);
        TimestampSpec timestampSpec = parseSpec.getTimestampSpec();
        DateTime dateTime = timestampSpec.extractTimestamp(event);
        return new MapBasedInputRow(dateTime, dimensions, event);
    }


    public final Map<String, Object> getEvent(final List<Field> fields, GenericRecord record) {
        final Map<String, Object> event = Maps.newHashMap();
        for (Field field : fields) {
            if (FieldType.MAP.equals(field.getFieldType())) {
                event.put(field.getKey().toString(),
                        processRecord(((Map<String, Object>) record.
                                get(field.getRootFieldName())).get(field.getKey())));
            } else if (FieldType.ARRAY.equals(field.getFieldType())) {
                event.put(field.getKey().toString(),
                        processRecord(((List<Object>) record.
                                get(field.getRootFieldName())).get(field.getIndex())));
            } else {
                event.put(field.getRootFieldName(), processRecord(record.get(field.getRootFieldName())));
            }
        }
        return event;
    }

    public static final Object processRecord(Object obj) {
//        if (obj != null) {
        if (obj instanceof Utf8) {
            return obj.toString();
        } else if (obj instanceof List) {
            ArrayList list = new ArrayList();
            for (Object value : (List) obj) {
                if (value instanceof GenericRecord)
                    list.add(((GenericRecord) value).get(0));
            }
            return list;
        } else if (obj instanceof Map) {
            Map map = new HashMap();
            Map incomingMap = (Map) obj;
            for (Object key : incomingMap.keySet()) {
                map.put(key, incomingMap.get(key));
            }
            return map;
        }
        return obj;
//        } else {
//            //Handling for dimensions with Empty event attributes
//            return EMPTY_STRING;
//        }
    }

    @JsonProperty
    @Override
    public ParseSpec getParseSpec() {
        return parseSpec;
    }

    @Override
    public InputRowParser withParseSpec(ParseSpec parseSpec) {
        return new ParquetHadoopInputRowParser(parseSpec, binaryAsString, parquetParserString);
    }

    public String getParquetParserString() {
        return parquetParserString;
    }

    public ParquetParser getParquetParser() {
        return parquetParser;
    }
}

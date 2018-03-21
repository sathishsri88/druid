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
import com.metamx.common.parsers.ParseException;
import io.druid.data.input.AvroStreamInputRowParser;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.data.input.parquet.model.Field;
import io.druid.data.input.parquet.model.ParquetParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ParquetHadoopInputRowParser implements InputRowParser<GenericRecord> {
    private final ParseSpec parseSpec;
    private final boolean binaryAsString;
    private final List<String> dimensions;
    private final ParquetParser parquetParser;

    @JsonCreator
    public ParquetHadoopInputRowParser(
            @JsonProperty("parseSpec") ParseSpec parseSpec,
            @JsonProperty("binaryAsString") Boolean binaryAsString,
            @JsonProperty("parquetParser") ParquetParser parquetParser
    ) {
        this.parseSpec = parseSpec;
        this.binaryAsString = binaryAsString == null ? false : binaryAsString;
        this.parquetParser = parquetParser;
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
        final Map<String, Object> event = getEvent(this.parquetParser.getFields(), record);
        final TimestampSpec timestampSpec = parseSpec.getTimestampSpec();
        final DateTime dateTime = timestampSpec.extractTimestamp(event);
        if (dateTime == null)
            throw new ParseException("Parsed datetime is null!");
        return new MapBasedInputRow(dateTime, dimensions, event);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getEvent(final List<Field> fields, GenericRecord record) {
        final Map<String, Object> event = Maps.newHashMap();
        for (Field field : fields) {
            if (field.getDimensionName() != null) {
                event.put(field.getDimensionName(), getFieldValue(field, record));
            } else {
                event.put(field.getKey().toString(), getFieldValue(field, record));
            }
        }
        return event;
    }

    @SuppressWarnings("unchecked")
    private Object getFieldValue(Field field, GenericRecord record) {
        switch (field.getFieldType()) {
            case MAP: {
                if (record.get(field.getRootFieldName()) != null) {
                    //Used while debugging non queryable attributes
                   /* System.out.println(String.format("Root Field Name : %s & Field key : %s "
                            , field.getRootFieldName(), field.getKey()));
                    System.out.println(String.format("Value :%s ",
                            ((Map<String, Object>) record.get(field.getRootFieldName())).get(field.getKey())));*/
                    return processRecord(((Map<Utf8, Object>) record.
                            get(field.getRootFieldName())).get(field.getKey()));
                }
                return null;
            }
            case ARRAY:
                return processRecord(((List<Object>) record.
                        get(field.getRootFieldName())).get(field.getIndex()));
            case FIELD:
                return getFieldValue(field.getField(), record);
            case UNION://
                final Map<Utf8, Object> baseMap = (Map<Utf8, Object>) record.get(field.getRootFieldName());
                if (baseMap != null) {
                    final GenericRecord genericRecord = (GenericRecord) ((Map<Utf8, Object>) baseMap).
                            get(field.getKey());
                    if (genericRecord != null) {
                        Object processObj = genericRecord.get(field.getField().getRootFieldName()) != null
                                ? genericRecord.get(field.getField().getRootFieldName())
                                : genericRecord.get(field.getField().getKey().toString());
                        if (processObj != null)
                            return processRecord(processObj);

                    }
                }
                return null;
            default:
                return processRecord(record.get(field.getKey().toString()));
        }
    }

    /**
     * Generic method returning value
     *
     * @param obj
     * @return
     */
    @SuppressWarnings("unchecked")
    private Object processRecord(Object obj) {
        if (obj instanceof Utf8) {
            return obj.toString();
        } else if (obj instanceof List) {
            // For de duping PARQUET arrays like [{"element": -1}]
            ArrayList list = new ArrayList();
            for (Object value : (List) obj) {
                if (value instanceof GenericRecord)
                    list.add(((GenericRecord) value).get(0));
            }
            return list;
        }
        //MAP handling with the current implementation is to return the same
//        else if (obj instanceof Map) {
//            return obj;
//        }
        else if (obj instanceof GenericRecord) {
            final Schema schema = ((GenericRecord) obj).getSchema();
            if (schema != null)
                for (Schema.Field field : schema.getFields()) {
                    Object value = ((GenericData.Record) obj).get(field.name());
                    if (value != null)
                        return value;
                }
        }
        return obj;
    }

    @JsonProperty
    @Override
    public ParseSpec getParseSpec() {
        return parseSpec;
    }

    @Override
    public InputRowParser withParseSpec(ParseSpec parseSpec) {
        return new ParquetHadoopInputRowParser(parseSpec, binaryAsString, parquetParser);
    }

    public ParquetParser getParquetParser() {
        return parquetParser;
    }
}

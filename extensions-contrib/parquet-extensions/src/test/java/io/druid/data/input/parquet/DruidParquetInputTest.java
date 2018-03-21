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

import com.google.common.collect.Lists;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.path.StaticPathSpec;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DruidParquetInputTest {
    @Test
    public void test() throws IOException, InterruptedException {
        HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File("example/wikipedia_hadoop_parquet_job.json"));
        Job job = Job.getInstance(new Configuration());
        config.intoConfiguration(job);
        GenericRecord data = getFirstRecord(job, "example/wikipedia_list.parquet");

        // field not read, should return null
        assertEquals(data.get("added"), null);
        assertEquals(data.get("page"), new Utf8("Gypsy Danger"));
        assertEquals(config.getParser().parse(data).getDimension("page").get(0), "Gypsy Danger");
    }

    @Test
    public void testBinaryAsString() throws IOException, InterruptedException {
        HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File("example/impala_hadoop_parquet_job.json"));
        Job job = Job.getInstance(new Configuration());
        config.intoConfiguration(job);
        GenericRecord data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());

        InputRow row = config.getParser().parse(data);
        // without binaryAsString: true, the value would something like "[104, 101, 121, 32, 116, 104, 105, 115, 32, 105, 115, 3.... ]"
        assertEquals(row.getDimension("field").get(0), "hey this is &é(-è_çà)=^$ù*! Ω^^");
        assertEquals(row.getTimestampFromEpoch(), 1471800234);
    }

    @Test
    public void testParquetMapParser() throws IOException, InterruptedException {
        HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File("example/map_parser_hadoop_parquet_job.json"));
        Job job = Job.getInstance(new Configuration());
        config.intoConfiguration(job);
        GenericRecord data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());

        InputRow row = config.getParser().parse(data);
        System.out.println(row);
        assertEquals(row.getDimension("framework_call_type").get(0), "si");
//        assertEquals(row.getDimension("cookie_id"),null);
        assertEquals(row.getDimension("qual_experiments").size(), 338);
    }

    @Test
    public void testElmoEventParquetParser() throws IOException, InterruptedException {
        HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File("example/elmo_all_attributes_hadoop_parquet_job.json"));
        Job job = Job.getInstance(new Configuration());
        config.intoConfiguration(job);
        GenericRecord data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());

        InputRow row = config.getParser().parse(data);
        System.out.println(row);
        assertEquals(row.getDimension("framework_call_type").get(0), "si");
//        assertEquals(row.getDimension("cookie_id"),null);
        assertEquals(row.getDimension("qual_experiments").size(), 338);
    }

    @Test
    public void testPrimitiveParquetProcessing() throws IOException, InterruptedException {
        HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File("example/elmo_checkout_with_primitives.json"));
        Job job = Job.getInstance(new Configuration());
        config.intoConfiguration(job);
        GenericRecord data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());

        InputRow row = config.getParser().parse(data);
        System.out.println(row);
    }

    @Test
    public void testHiveParquetFile() throws IOException, InterruptedException {
        HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File("example/spec/checkoutDruidSpec2017-07-25_16_40_33.json"));
        Job job = Job.getInstance(new Configuration());
        config.intoConfiguration(job);
        GenericRecord data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());

        InputRow row = config.getParser().parse(data);
        System.out.println(row);
    }

    @Test
    public void testParquetMapParserWithEmptyAttrs() throws IOException, InterruptedException {
        HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File("example/map_empty_attrs_hadoop_parquet_job.json"));
        Job job = Job.getInstance(new Configuration());
        config.intoConfiguration(job);
        GenericRecord data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());

        InputRow row = config.getParser().parse(data);
        System.out.println(row);
//        assertEquals(row.getDimension("framework_call_type").get(0),"si");
//        assertEquals(row.getDimension("cookie_id"),null);
//        assertEquals(row.getDimension("qual_experiments").size(),338);
    }

    @Test
    public void testELMOCldIngestionParser() throws IOException, InterruptedException {
        HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File("example/spec/pangea_cust_daily_ingest_spec.json"));
        Job job = Job.getInstance(new Configuration());
        config.intoConfiguration(job);
        GenericRecord data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());

        InputRow row = config.getParser().parse(data);
        System.out.println(row);
    }

    @Test
    public void testPangeaCldIngestionParser() throws IOException, InterruptedException {
        HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File("example/spec/pangea_cust_daily_ingest_spec.json"));
        Job job = Job.getInstance(new Configuration());
        config.intoConfiguration(job);
        List<GenericRecord> genericRecords = getNRecords(job, ((StaticPathSpec) config.getPathSpec()).getPaths(), 200);
        List<MapBasedInputRow> parsedRecords = Lists.newArrayList();
        List<String> mapKeys = Lists.newArrayList("attributes", "cldAttributes", "cometAttributes");
        for (GenericRecord record : genericRecords) {
            parsedRecords.add((MapBasedInputRow) config.getParser().parse(record));
        }
        for (int i = 0; i < parsedRecords.size(); i++) {
            GenericRecord genericRecord = genericRecords.get(i);
            Map<String, Map<String, Object>> attributesMap = new HashMap<>();
            for (String key : mapKeys) {
                attributesMap.put(key, (Map<String, Object>) genericRecord.get(key));
            }
            MapBasedInputRow parsedRecord = parsedRecords.get(i);
            for (String key : parsedRecord.getEvent().keySet()) {
                Object parsedValue = parsedRecord.getEvent().get(key);
                Object eventValue = getValue(key, attributesMap);
                if (eventValue != null) {
                    try {
                        boolean equals = eventValue.toString().equals(parsedValue);
                        if (!equals)
                            System.out.println("Unequality b/w key : " + key + " Value (a,b) : (" + parsedValue + "," + eventValue + ")");
//                        else
//                            System.out.println("Equality b/w key : " + key + " Value (a,b) : (" + parsedValue + "," + eventValue + ")");
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("Unequality b/w key : " + key + " Value (a,b) : (" + parsedValue + "," + eventValue + ")");
                    }
                }
//                else
//                    System.out.println("EventValue NULL b/w key : " + key + " Value (a) : (" + eventValue + ")");
            }
        }
        System.out.println(parsedRecords.get(0));
    }


    @Test
    public void testElmoClientIngestionParser() throws IOException, InterruptedException {
        HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File("example/spec/elmo_client_druid_intra_spec.json"));
        Job job = Job.getInstance(new Configuration());
        config.intoConfiguration(job);
        List<GenericRecord> genericRecords = getNRecords(job, ((StaticPathSpec) config.getPathSpec()).getPaths(), 20000000);
        List<MapBasedInputRow> parsedRecords = Lists.newArrayList();
        List<String> mapKeys = Lists.newArrayList("attributes", "cldAttributes", "cometAttributes");
        for (GenericRecord record : genericRecords) {
            parsedRecords.add((MapBasedInputRow) config.getParser().parse(record));
        }
        for (int i = 0; i < parsedRecords.size(); i++) {
            GenericRecord genericRecord = genericRecords.get(i);
            Map<String, Map<String, Object>> attributesMap = new HashMap<>();
            for (String key : mapKeys) {
                attributesMap.put(key, (Map<String, Object>) genericRecord.get(key));
            }
            MapBasedInputRow parsedRecord = parsedRecords.get(i);
            for (String key : parsedRecord.getEvent().keySet()) {
                Object parsedValue = parsedRecord.getEvent().get(key);
                Object eventValue = getValue(key, attributesMap);
                if (eventValue != null) {
                    try {
                        boolean equals = eventValue.toString().equals(parsedValue);
                        if (!equals)
                            System.out.println("Unequality b/w key : " + key + " Value (a,b) : (" + parsedValue + "," + eventValue + ")");
//                        else
//                            System.out.println("Equality b/w key : " + key + " Value (a,b) : (" + parsedValue + "," + eventValue + ")");
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("Unequality b/w key : " + key + " Value (a,b) : (" + parsedValue + "," + eventValue + ")");
                    }
                }
//                else
//                    System.out.println("EventValue NULL b/w key : " + key + " Value (a) : (" + eventValue + ")");
            }
        }
        System.out.println(parsedRecords.get(0));
    }

    private Object getValue(String key, Map<String, Map<String, Object>> attributesMap) {
        if (attributesMap != null) {
            for (String mapKey : attributesMap.keySet()) {
                Map<String, Object> attributes = attributesMap.get(mapKey);
                if (attributes != null && attributes.containsKey(new Utf8(key))) {
                    return attributes.get(new Utf8(key));
//                    if (attributes.get(key) != null) {
//                        return attributes.get(key).toString();
//                    }
                }
            }
        }
        return null;
    }

    @Test
    public void testExtractNestedMap() throws IOException, InterruptedException {
        HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File("example/spec/pangea_cld_ingest_spec_ts_extract.json"));
        Job job = Job.getInstance(new Configuration());
        config.intoConfiguration(job);
        GenericRecord data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());

        InputRow row = config.getParser().parse(data);
        System.out.println(row);
    }

    private GenericRecord getFirstRecord(Job job, String parquetPath) throws IOException, InterruptedException {
        File testFile = new File(parquetPath);
        Path path = new Path(testFile.getAbsoluteFile().toURI());
        FileSplit split = new FileSplit(path, 0, testFile.length(), null);

        DruidParquetInputFormat inputFormat = ReflectionUtils.newInstance(DruidParquetInputFormat.class, job.getConfiguration());
        TaskAttemptContext context = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
        RecordReader reader = inputFormat.createRecordReader(split, context);

        reader.initialize(split, context);
//        reader.nextKeyValue();
        int cnt = 0;
        while (reader.nextKeyValue() && cnt < 150) {
            System.out.println(reader.getCurrentKey() + " - " + reader.getCurrentValue());
            cnt++;
        }
        GenericRecord data = (GenericRecord) reader.getCurrentValue();
        reader.close();
        return data;
    }

    private List<GenericRecord> getNRecords(Job job, String parquetPath, int size) throws IOException, InterruptedException {
        File testFile = new File(parquetPath);
        Path path = new Path(testFile.getAbsoluteFile().toURI());
        FileSplit split = new FileSplit(path, 0, testFile.length(), null);

        DruidParquetInputFormat inputFormat = ReflectionUtils.newInstance(DruidParquetInputFormat.class, job.getConfiguration());
        TaskAttemptContext context = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
        RecordReader reader = inputFormat.createRecordReader(split, context);

        reader.initialize(split, context);
        List<GenericRecord> records = Lists.newArrayListWithCapacity(size);
        int cnt = 0;
        while (reader.nextKeyValue() && cnt < size) {
//            System.out.println(reader.getCurrentKey() + " - " + reader.getCurrentValue());
            records.add((GenericRecord) reader.getCurrentValue());
            cnt++;
        }
        reader.close();
        return records;
    }

}

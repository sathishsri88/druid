package io.druid.data.input.parquet;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.input.parquet.model.Field;
import io.druid.data.input.parquet.model.FieldType;
import org.apache.avro.util.Utf8;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


public class GenerateParserSpecUtils {

    @Test
    public void testGenerateFieldsFromString() {
        String input = "[\"timestamp\",\"attributesMap[component_name]\",\"attributesMap[page_name]\",\"attributesMap[page_group]\",\"attributesMap[os]\",\"attributesMap[browser_version]\",\"attributesMap[browser_type]\",\"attributesMap[account_type]\",\"attributesMap[device_id]\",\"attributesMap[device]\",\"attributesMap[visitor_id]\",\"attributesMap[cookie_id]\",\"attributesMap[bot]\",\"attributesMap[decr_account_number]\",\"experiencedExperiments\",\"experiencedTreatments\",\"hashMod100\",\"experimentVersions\",\"treatmentVersions\",\"attributesMap[country_iso_code]\",\"attributesMap[country_name]\",\"attributesMap[continent]\",\"attributesMap[event_type]\"]";
        List<String> fields = JsonUtils.readFrom(input, List.class);
        List<io.druid.data.input.parquet.model.Field> parsedFields = io.druid.data.input.parquet.model.Field.parseFields(fields);
        List<Field> rs = new ArrayList<Field>();
        for (io.druid.data.input.parquet.model.Field field : io.druid.data.input.parquet.model.Field.parseFields(fields)) {
            rs.add(new Field(field.getRootFieldName(), field.getIndex(), field.getKey(), field.getFieldType(), field.getField()));
        }
        String json = JsonUtils.writeToString(rs);
        System.out.println(json);
    }

    private static class Field {
        private final String rootFieldName;
        private final int index;
        private final String key;
        private final FieldType fieldType;
        private final io.druid.data.input.parquet.model.Field field;

        private Field(String rootFieldName,
                      int index,
                      Utf8 key,
                      FieldType fieldType,
                      io.druid.data.input.parquet.model.Field field) {
            this.rootFieldName = rootFieldName;
            this.index = index;
            this.key = key.toString();
            this.fieldType = fieldType;
            this.field = field;
        }

        public String getRootFieldName() {
            return rootFieldName;
        }

        public int getIndex() {
            return index;
        }

        public String getKey() {
            return key;
        }

        public FieldType getFieldType() {
            return fieldType;
        }

        public io.druid.data.input.parquet.model.Field getField() {
            return field;
        }
    }
}

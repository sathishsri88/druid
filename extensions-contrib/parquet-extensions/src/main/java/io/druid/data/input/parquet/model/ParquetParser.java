package io.druid.data.input.parquet.model;

import java.util.List;

/**
 * Created by sathsrinivasan on 11/4/16.
 */
public class ParquetParser {

    private List<String> fields;

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    private transient List<Field> parsedFields;

    public List<Field> getParsedFields() {
        return parsedFields;
    }

    public void setParsedFields(List<Field> parsedFields) {
        this.parsedFields = parsedFields;
    }

    public boolean containsType(String typeName) {
        for (Field field : this.getParsedFields()) {
            if (field.getRootFieldName().equals(typeName))
                return true;
        }
        return false;
    }
}

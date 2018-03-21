package io.druid.data.input.parquet.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Created by sathsrinivasan on 11/4/16.
 */
public class ParquetParser {

    @JsonCreator
    public ParquetParser(@JsonProperty("fields") List<Field> fields) {
        this.fields = fields;
    }

    private final List<Field> fields;

    public List<Field> getFields() {
        return fields;
    }

    private transient List<Field> parsedFields;

    public boolean containsType(String typeName) {
        for (Field field : this.getFields()) {
            if (field.getKey().toString().equals(typeName) || (field.getRootFieldName() != null &&
                    field.getRootFieldName().equals(typeName)))
                return true;
        }
        return false;
    }
}

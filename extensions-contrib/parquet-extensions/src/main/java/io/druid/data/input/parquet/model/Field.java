package io.druid.data.input.parquet.model;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by sathsrinivasan on 11/4/16.
 */
public final class Field {

    public static final Utf8 EMPTY_STR = new Utf8("");
    public static final int DEF_INDEX = -1;

    private final String rootFieldName;
    private final int index;
    private final Utf8 key;
    private final FieldType fieldType;

    private Field(String rootFieldName, int index, Utf8 key, FieldType fieldType) {
        this.rootFieldName = rootFieldName;
        this.index = index;
        this.key = key;
        this.fieldType = fieldType;
    }

    public String getRootFieldName() {
        return rootFieldName;
    }

    public int getIndex() {
        return index;
    }

    public Utf8 getKey() {
        return key;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    private static final Pattern SQUARE_PATTERN = Pattern.compile("\\[(.*?)\\]");
    private static final Pattern CURLY_PATTERN = Pattern.compile("\\((.*?)\\)");

    public static final Field parseField(String field) {
        Preconditions.checkNotNull(field, "Nullable field not expected while parsing field");
        if (field.contains("[")) {
            return new Field(field.substring(0, field.indexOf("[")), -1,
                    extractKey(SQUARE_PATTERN, field), FieldType.MAP);
        } else if (field.contains(("("))) {
            final Utf8 key = extractKey(CURLY_PATTERN, field);
            if (StringUtils.isNumeric(key)) {
                return new Field(field.substring(0, field.indexOf("(")), Integer.parseInt(key.toString()),
                        key, FieldType.MAP);
            } else {
                return new Field(field.substring(0, field.indexOf("(")), -1,
                        key, FieldType.MAP);
            }
        } else {
            return new Field(field, DEF_INDEX, EMPTY_STR, FieldType.STRING);
        }
    }

    public static final List<Field> parseFields(List<String> fields) {
        final List<Field> parsedFields = Lists.newArrayList();
        if (fields != null && !fields.isEmpty()) {
            for (String field : fields) {
                parsedFields.add(parseField(field));
            }
        }
        return parsedFields;
    }

    public static final Utf8 extractKey(Pattern pattern, String field) {
        final Matcher matcher = pattern.matcher(field);
        while (matcher.find()) {
            return new Utf8(matcher.group(1));
        }
        return EMPTY_STR;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Field field = (Field) o;

        if (index != field.index) return false;
        if (rootFieldName != null ? !rootFieldName.equals(field.rootFieldName) : field.rootFieldName != null)
            return false;
        if (key != null ? !key.equals(field.key) : field.key != null) return false;
        return fieldType == field.fieldType;

    }

    @Override
    public int hashCode() {
        int result = rootFieldName != null ? rootFieldName.hashCode() : 0;
        result = 31 * result + index;
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (fieldType != null ? fieldType.hashCode() : 0);
        return result;
    }
}

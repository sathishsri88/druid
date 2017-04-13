package io.druid.data.input.parquet;

import com.google.gson.Gson;

/**
 * Created by sathsrinivasan on 11/4/16.
 */
public class JsonUtils {

    public static final <T> T readFrom(String json, Class<T> t) {
        return new Gson().fromJson(json, t);
    }
}

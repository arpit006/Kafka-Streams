package com.kafka.streamsapp.parser;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kafka.streamsapp.util.JsonUtil;

import java.util.Map;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class JsonParser {

    /**
     * For simplicity parsing the simple JSON and returning the parsed JSON as a Map
     * @param inJson json
     * @return map of json values.
     */
    public static Map<String, Object> parseJson(String inJson) {
        return JsonUtil.fromJson(inJson, new TypeReference<Map<String, Object>>() {
        });
    }
}

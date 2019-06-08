package com.kafka.streamsapp.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;

import static com.fasterxml.jackson.core.JsonParser.Feature;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class JsonUtil {

    private static ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.configure(Feature.ALLOW_COMMENTS, true);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
    }

    private static ObjectMapper getObjectMapper() {
        return mapper;
    }

    public static <T> T fromJson(String inJson, Class<T> clazz) {
        T t = null;
        try {
            t = getObjectMapper().readValue(inJson, clazz);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return t;
    }

    public static <T> T fromJson(String inJson, TypeReference<T> typeReference) {
        T t = null;
        try {
            t = getObjectMapper().readValue(inJson, new TypeReference<T>() {});
        } catch (IOException e) {
            e.printStackTrace();
        }
        return t;
    }

    public static String toJson(Object inValue) {
        String s = null;
        try {
            s= getObjectMapper().writeValueAsString(inValue);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return s;
    }
}

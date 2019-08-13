package com.zzh.utils;

import com.google.gson.Gson;

import java.nio.charset.Charset;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-13 11:45
 **/
public class GsonUtil {
    private final static Gson gson = new Gson();

    public static <T> T fromJson(String value, Class<T> type) {
        return gson.fromJson(value, type);
    }

    public static String toJson(Object value) {
        return gson.toJson(value);
    }

    public static byte[] toJSONBytes(Object value) {
        return gson.toJson(value).getBytes(Charset.forName("UTF-8"));
    }
}

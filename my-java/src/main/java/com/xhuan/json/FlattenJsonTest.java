package com.xhuan.json;

import com.github.wnameless.json.base.GsonJsonCore;
import com.github.wnameless.json.flattener.FlattenMode;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.github.wnameless.json.unflattener.JsonUnflattener;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.Map;

/**
 * @Classname FlattenJsonTest
 * @Description 扁平化json字符串
 * @Version 1.0.0
 * @Date 2023/3/3 9:48
 * @Created by xiaohuan
 */
public class FlattenJsonTest {

    public static void main(String[] args) {
        String json = "{\"code\":200,\"msg\":\"ok\",\"data\":{\"records\":[{\"id\":7,\"nickname\":\"test\",\"username\":\"test1\",\"birthday\":\"2012-06-08T00:00:00\"},{\"id\":8,\"nickname\":\"test2\",\"username\":\"test2\",\"birthday\":\"2012-06-09T00:00:00\"}],\"total\":1,\"pages\":1,\"current\":1,\"size\":20}}";

        // 扁平化成map
        Map<String, Object> flattenJson = JsonFlattener.flattenAsMap(json);

        System.out.println(flattenJson);

        // 扁平化成json字符串
        String jsonStr = JsonFlattener.flatten(json);

        System.out.println(jsonStr);

        // 去扁平化
        String nestedJson = JsonUnflattener.unflatten(jsonStr);

        System.out.println(nestedJson);

        System.out.println(new JsonFlattener(json).withFlattenMode(FlattenMode.MONGODB).flatten());

        // FlattenMode.KEEP_ARRAYS - flatten all except arrays
        System.out.println(new JsonFlattener(json).withFlattenMode(FlattenMode.KEEP_ARRAYS).flatten());
        // {"abc.def":[1,2,{"g.h":[3]}]}

        // When the flattened outcome can NOT suit in a Java Map, it will still be put in the Map with "root" as its key.
        Map<String, Object> map = new JsonFlattener(json).withFlattenMode(FlattenMode.KEEP_ARRAYS).flattenAsMap();
        System.out.println(map);
        

//        Gson gson = new GsonBuilder().serializeNulls().create();
//        JsonFlattener jf = new JsonFlattener(new GsonJsonCore(gson), json);
//        System.out.println(jf.flattenAsMap());
//        JsonUnflattener ju = new JsonUnflattener(new GsonJsonCore(gson), json);
    }
}

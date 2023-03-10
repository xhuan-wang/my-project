package com.xhuan.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.Map;

/**
 * @Classname FlattenJson
 * @Description TODO
 * @Version 1.0.0
 * @Date 2023/3/3 10:11
 * @Created by xiaohuan
 */
public class FlattenJson {
    public static void main(String[] args) {

        String json = "{\"code\":200,\"msg\":\"ok\",\"data\":{\"records\":[{\"id\":7,\"nickname\":\"test\",\"username\":\"test1\",\"birthday\":\"2012-06-08T00:00:00\"},{\"id\":8,\"nickname\":\"test2\",\"username\":\"test2\",\"birthday\":\"2012-06-09T00:00:00\"}],\"total\":1,\"pages\":1,\"current\":1,\"size\":20}}";

        JsonParser parser = new JsonParser();
        JsonElement tree = parser.parse(json);
        //System.out.println(tree.isJsonObject());//true
        JsonObject jo = (JsonObject)tree;
        JsonObject fa = new JsonObject();
        String father = "";
        //System.out.println(flatten(jo,fa,father));
        JsonObject resultJo = flatten(jo,fa,father);
        for(Map.Entry entry : resultJo.entrySet()){
            System.out.println(entry.getKey().toString());
            System.out.println(entry.getValue().toString());
        }

    }


    private static JsonObject flatten(JsonObject object, JsonObject flattened, String father){
        //flatten递归函数实现对多层json的扁平化处理解析，第三个形参仅仅用来保留外层的键并在之后进行拼接

        if(flattened == null){
            flattened = new JsonObject();
        }
        // Iterator<String> keys =(Iterator<String>) object.keySet();
        for(Map.Entry entry : object.entrySet()){
            String midFather = entry.getKey().toString();
            String tmp = father;
            JsonElement tmpVa = (JsonElement) entry.getValue();
            try{
                if(tmpVa.isJsonObject()){
                    //检测到多层json的时候进行递归处理
                    tmp = tmp +"." + midFather;//当前层键与之前外层键进行拼接
                    //System.out.println("aaa"+entry.getKey().toString()+"--------"+tmp);
                    flatten(object.getAsJsonObject(entry.getKey().toString()),flattened,tmp);
                } else {
                    //当前层的值没有嵌套json键值对，直接将键值对添加到flattened中

                    String nowKeyTmp = father + "." + entry.getKey().toString();
                    String nowKey = nowKeyTmp.substring(1,nowKeyTmp.length());
                    flattened.add(nowKey,((JsonElement) entry.getValue()));
                }
            } catch (JsonIOException e) {
                System.out.println(e);
            }
        }

        return flattened;
    }
}

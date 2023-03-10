package com.xhuan.compiler;

import com.alibaba.fastjson.JSON;
import com.itranswarp.compiler.JavaStringCompiler;

import java.lang.reflect.Field;
import java.util.*;

/**
 * @Classname TestCompiler
 * @Description TODO
 * @Version 1.0.0
 * @Date 2023/3/1 17:18
 * @Created by xiaohuan
 */
public class TestCompiler {
    /**
     * 编译java代码，并生成对象
     *
     * @param packageName 包名
     * @param className   类名
     * @param javaCode    源码
     * @return
     * @throws Exception
     */
    public static Object createBean(String packageName, String className, String javaCode) throws Exception {
        JavaStringCompiler compiler = new JavaStringCompiler();
        Map<String, byte[]> result = compiler.compile(className + ".java", "package " + packageName + ";\n" + javaCode);
        Class clz = compiler.loadClass(packageName + "." + className, result);
        return clz.newInstance();
    }

    private static String genCode(String clzName, Map<String, Object> map) {
        StringBuilder builder = new StringBuilder("public class ").append(clzName).append("{\n");
        for (Map.Entry<String, Object> obj: map.entrySet()) {
            String fieldType;
            if (obj.getValue() instanceof List) {
                fieldType = "java.util.List";
            } else if (obj.getValue() instanceof Set) {
                fieldType = "java.util.Set";
            } else if (obj.getValue() instanceof  Map) {
                fieldType = "java.util.Map";
            } else {
                fieldType = obj.getValue().getClass().getName().replace("$", ".");
            }
            builder.append("\tpublic ").append(fieldType).append(" ").append(obj.getKey()).append(";\n");
        }
        builder.append("}");
        return builder.toString();
    }

    public static void main(String[] args) throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("hello", "world");
        map.put("key", 12);
        map.put("list", Arrays.asList(1, 2, 3));
        String packageName = "com.git.hui.dynamic";
        String clzName = "MBean1";
        String code = genCode(clzName, map);
        Object bean = createBean(packageName, clzName, code);
        // 初始化bean的成员
        for(Map.Entry<String, Object> entry: map.entrySet()) {
            Field field = bean.getClass().getField(entry.getKey());
            field.set(bean, entry.getValue());
        }
        System.out.println("---------------- java code  ---------------\n" + code + "\n------------------");
        System.out.println(JSON.toJSONString(bean));
    }
}

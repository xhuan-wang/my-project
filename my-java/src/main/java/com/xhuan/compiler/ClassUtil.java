package com.xhuan.compiler;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.HashMap;
import java.util.Map;

/**
 * @Classname ClassUtil
 * @Description TODO
 * @Version 1.0.0
 * @Date 2023/3/2 15:14
 * @Created by xiaohuan
 */
public class ClassUtil {
    public static void main(String[] args) {
        String s = "id";
        System.out.println(s.split("\\.").length);
    }
}

package com.xhuan.compiler;

import net.sf.cglib.beans.BeanGenerator;
import net.sf.cglib.beans.BeanMap;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @Classname DynamicBean
 * @Description TODO
 * @Version 1.0.0
 * @Date 2023/3/2 15:04
 * @Created by xiaohuan
 */
public class DynamicBean {

    private Object object = null; //动态生成的类
    private BeanMap beanMap = null; //存放属性名称以及属性的类型

    public DynamicBean() {
        super();
    }

    public DynamicBean(Map propertyMap) {
        this.object = generateBean(propertyMap);
        this.beanMap = BeanMap.create(this.object);
    }

    private Object generateBean(Map propertyMap) {
        BeanGenerator generator = new BeanGenerator();
        Set keySet = propertyMap.keySet();
        for (Iterator i = keySet.iterator(); i.hasNext();) {
            String key = (String) i.next();
            generator.addProperty(key, (Class) propertyMap.get(key));
        }
        return generator.create();
    }

    /**
     * @description 给属性赋值
     * @param property
     * @param value
     * @return void
     * @author xiaohuan
     * @date 2023/3/2 15:11:18
     */
    public void setValue(Object property, Object value) {
        beanMap.put(property, value);
    }

    /**
     * @description 通过属性名获取属性值
     * @param property
     * @return java.lang.Object
     * @author xiaohuan
     * @date 2023/3/2 15:12:20
     */
    public Object getValue(String property) {
        return beanMap.get(property);
    }

    /**
     * @description 得到实体bean对象
     *
     * @return java.lang.Object
     * @author xiaohuan
     * @date 2023/3/2 15:13:04
     */
    public Object getObject(){
        return this.object;
    }
}

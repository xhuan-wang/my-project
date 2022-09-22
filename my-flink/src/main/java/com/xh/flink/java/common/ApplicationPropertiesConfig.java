package com.xh.flink.java.common;

import java.io.InputStream;
import java.util.Properties;

public class ApplicationPropertiesConfig {
    private static final String configPath="/application.properties";
    private Properties propertyFile=new Properties();

    /**
     *构造类时加载配置文件
     **/
    public ApplicationPropertiesConfig(){
        try{
            //toURI()解决中文路径问题
//            String path=this.getClass().getClassLoader().getResource(this.configPath).toURI().getPath();
//            InputStream in=new FileInputStream(path);
            InputStream in = this.getClass().getResourceAsStream(this.configPath);
//            String path="D:\\项目\\数据支撑\\【数据同步】Flink-Flink CDC\\Flink CDC-相关软件\\flink-connector-redis-main\\\\target\\classes\\application.properties";

            propertyFile.load(in);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public String getProperty(String key){
        return propertyFile.getProperty(key);
    }
}

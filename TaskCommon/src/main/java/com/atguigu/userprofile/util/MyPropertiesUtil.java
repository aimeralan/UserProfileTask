package com.atguigu.userprofile.util;

import java.io.InputStreamReader;
import java.util.Properties;

public class MyPropertiesUtil {

    public static void main(String[] args) {
        Properties properties = MyPropertiesUtil.load("config.properties");
        System.out.println(properties.getProperty("mysql.url"));
    }


    public static Properties load(String propertieFileName) {
        Properties prop = new Properties();
        try {
            prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader().
                    getResourceAsStream(propertieFileName), "UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("未找到文件:" + propertieFileName);
        }
        return prop;

    }
}

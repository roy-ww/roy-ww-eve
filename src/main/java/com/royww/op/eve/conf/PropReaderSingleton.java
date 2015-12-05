package com.royww.op.eve.conf;

import java.util.Set;

/**
 * PropertiesReader的单例实现
 * Created by roy.ww on 2015/12/5.
 */
public class PropReaderSingleton {
    private static PropReaderSingleton ourInstance = new PropReaderSingleton();
    private static PropertiesReader propertiesReader = new PropertiesReader();

    public static PropReaderSingleton getInstance() {
        return ourInstance;
    }

    private PropReaderSingleton() {
    }

    public static PropertiesReader load(String resource){
        return propertiesReader.loadPropertie(resource);
    }
    public static String get(String key) {
        return propertiesReader.get(key);
    }

    public static int getInt(String key) {
        return propertiesReader.getInt(key);
    }

    public static long getLong(String key) {
        return propertiesReader.getLong(key);
    }

    public static boolean exist(String key) {
        return propertiesReader.exist(key);
    }
    public static Set<String> getKeys() {
        return propertiesReader.getKeys();
    }

}

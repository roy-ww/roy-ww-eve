package com.royww.op.eve.conf;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
/**
 * Properties文件读取器
 * Created by roy.ww on 2015/12/5.
 * <p>
 * 实现加载 properties & ini 文件
 * </p>
 */
public class PropertiesReader {
    Logger logger = LoggerFactory.getLogger(PropertiesReader.class);
    static Map<String, String> values = Maps.newHashMap();
    static Set<String> loadedPaths = Sets.newHashSet();

    final static Set<String> fileExtensions = Sets.newHashSet("properties", "ini");

    public PropertiesReader(final List<String> paths) {
        loadProperties(paths);
    }

    public PropertiesReader(){};

    /**
     * 加载配置文件
     * @param resource 相对资源目录，如项目根目录conf下的default.properties 应为 conf/default.properties
     * @return
     */
    public PropertiesReader loadPropertie(String resource){
        loadProperties(Lists.newArrayList(resource));
        return this;
    }
    private Map<String, String> loadAndParsePropertiesFile(String filePath) throws IOException {
        /**
         * 防止加载过的文件重复加载
         */
        if(loadedPaths.contains(filePath)){
            return Maps.newHashMap();
        }
        loadedPaths.add(filePath);

        Map<String, String> props = Files.readLines(new File(filePath), Charsets.UTF_8,
                new LineProcessor<Map<String, String>>() {
                    final Map<String, String> kvs = Maps.newHashMap();

                    public boolean processLine(String s) throws IOException {
                        int equalsIdx = s.indexOf("=");
                        if (equalsIdx < 0) {
                            kvs.put(s.trim(), null);
                        } else {
                            kvs.put(s.substring(0, equalsIdx).trim(), readUnicodeStr2(s.substring(equalsIdx + 1, s.length())).trim());
                        }
                        return true;
                    }

                    public Map<String, String> getResult() {
                        return kvs;
                    }
                });
        return props;
    }

    private void loadProperties(final List<String> paths) {
        Set<String> propFiles = Sets.newHashSet();
        for (String u : paths) {
            File f;
            try {
                //fix by shuzhe.ssz change "%20" to " ",waiting for another good solution.，
                f = new File(URLDecoder.decode(Resources.getResource(u).getPath(),Charsets.UTF_8.name()));
                if (f.isDirectory()) {
                    FluentIterable<File> iterable = Files.fileTreeTraverser().breadthFirstTraversal(f);
                    for (File childrenFile : iterable) {
                        if (!childrenFile.isDirectory() &&
                                fileExtensions.contains(Files.getFileExtension(childrenFile.getPath()))) {
                            propFiles.add(childrenFile.getPath());
                        }
                    }
                } else if (fileExtensions.contains(Files.getFileExtension(f.getPath()))) {
                    propFiles.add(f.getPath());
                }
            } catch (UnsupportedEncodingException e) {
                logger.error("load properties file error.Encoding=utf-8", e);
            }
        }
        for (String filePath : propFiles) {
            try {
                values.putAll(loadAndParsePropertiesFile(filePath));
            } catch (IOException e) {
                logger.error("load properties file error.filePath={}", filePath, e);
            }
        }
    }

    /**
     * 得到所有的KEY
     * @return
     */
    public Set<String> getKeys(){
        Set<String> keys = values.keySet();
        return keys==null?new HashSet<String>():keys;
    }

    public String get(String key) {
        String v = values.get(key);
        Preconditions.checkNotNull(v, "K-V not exist.key=" + key);
        return values.get(key);
    }

    public int getInt(String key) {
        return Integer.parseInt(get(key));
    }

    public long getLong(String key) {
        return Long.parseLong(get(key));
    }

    public boolean exist(String key){
        return values.containsKey(key);
    }

    public Map<String,String> getAllConf(){
        return values;
    }

    private String readUnicodeStr2(String unicodeStr) {
        StringBuilder buf = new StringBuilder();

        for (int i = 0; i < unicodeStr.length(); i++) {
            char char1 = unicodeStr.charAt(i);
            if (char1 == '\\' && isUnicode(unicodeStr, i)) {
                String cStr = unicodeStr.substring(i + 2, i + 6);
                int cInt = Integer.parseInt(cStr,16);
                buf.append((char) cInt);
                // 跨过当前unicode码，因为还有i++，所以这里i加5，而不是6
                i = i + 5;
            } else {
                buf.append(char1);
            }
        }
        return buf.toString();
    }

    // 判断以index从i开始的串，是不是unicode码
    private boolean isUnicode(String unicodeStr, int i) {

        int len = unicodeStr.length();
        int remain = len - i;
        // unicode码，反斜杠后还有5个字符 uxxxx
        if (remain < 5)
            return false;

        char flag2 = unicodeStr.charAt(i + 1);
        if (flag2 != 'u')
            return false;
        String nextFour = unicodeStr.substring(i + 2, i + 6);
        return isHexStr(nextFour);
    }

    /** hex str 0-9 a-f A-F */
    private boolean isHexStr(String str) {
        for (int i = 0; i < str.length(); i++) {
            char ch = str.charAt(i);
            boolean isHex = (ch >= '0'&&ch<='9')||(ch>='a'&&ch<='f')||(ch >= 'A'&&ch <= 'F');
            if (!isHex)
                return false;
        }
        return true;
    }
}

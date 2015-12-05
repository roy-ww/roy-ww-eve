package com.royww.op.eve.filter;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.royww.op.eve.conf.PropReaderSingleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * IP防火墙
 * Created by roy.ww on 2015/12/5.
 */
public class IPFireWallFilter implements Filter {

    static Logger logger = LoggerFactory.getLogger(IPFireWallFilter.class);
    private final static String CONFIG_FILE_PARAMETER_NAME = "conf"; // 过滤器配置文件参数名
    private final static String CONFIG_NAMESPACE_FILE_PARAMETER_NAME = "namespace"; // 过滤器配置文件参数名
    private final static String DEFAULT_CONFIG_FILE = "filter_conf.properties"; // 默认配置文件
    private final static String WHITE_LIST_IP_CONF_SPACE = "com.autonavi.aos.common.filter.white.ip"; // 白名单配置

    private final static String IP_BLOCKER_PARAMETER_NAME = "ipBlocker";

    private InterceptHandler interceptHandler;

    static List<RangeSet<Integer>> ipRangeConfiguration = Lists.newArrayList();
    static{
        for(int i=0;i<4;i++){
            RangeSet<Integer> rangeSet = TreeRangeSet.create();
            ipRangeConfiguration.add(rangeSet);
        }
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        String conf = filterConfig.getInitParameter(CONFIG_FILE_PARAMETER_NAME);
        String namespace = filterConfig.getInitParameter(CONFIG_NAMESPACE_FILE_PARAMETER_NAME);
        Preconditions.checkArgument(!(Strings.isNullOrEmpty(namespace)),
                "未查找到白名单配置参数 ConfName=" + CONFIG_NAMESPACE_FILE_PARAMETER_NAME);

        if (Strings.isNullOrEmpty(conf)) {
            PropReaderSingleton.load(DEFAULT_CONFIG_FILE);
        } else {
            PropReaderSingleton.load(conf);
        }
        Set<String> keys = PropReaderSingleton.getKeys();
        for(String k:keys){
            if(k.startsWith(WHITE_LIST_IP_CONF_SPACE+"."+namespace)){
                parseConfiguration(k,PropReaderSingleton.get(k));
            }
        }

        try{
            String ipBlockerClass = filterConfig.getInitParameter(IP_BLOCKER_PARAMETER_NAME);
            if(!Strings.isNullOrEmpty(ipBlockerClass)){
                //读取自定义的IPBlocker实现
                interceptHandler = (InterceptHandler)Class.forName(ipBlockerClass).newInstance();
            }else{
                /*
                * 默认的IPBlocker实现
                 */
                interceptHandler = new InterceptHandler() {
                    @Override
                    public void handle(String clientIp, ServletResponse servletResponse) throws IOException {
                        HttpServletResponse response = (HttpServletResponse) servletResponse;
                        response.setHeader("Content-type", "text/html;charset=UTF-8");
                        response.getOutputStream().write("Don't have authorization to access".getBytes("UTF-8"));
                        response.getOutputStream().flush();
                        response.getOutputStream().close();
                    }
                };
            }
        }catch (Exception e){
            logger.info("IPBlock parameter is illegal.class={}",filterConfig.getInitParameter(IP_BLOCKER_PARAMETER_NAME));
        }
    }

    /**
     * 解析IP配置
     * @param ips 192.168.1.1 192.168.*.1 192.168.1.[0-200]
     */
    private void parseConfiguration(String key,String ips){
        List<String> ipList = Splitter.on(",").splitToList(ips);
        for(String ip:ipList){
            List<String> itemList = Splitter.on(".").splitToList(ip.trim());
            if(itemList.size()!=4){
                logger.error("Illegal white-list ip config.key={} value={} illegal={}",key,ips,ip);
            }
            try{
                List<Integer[]> ipSegment = Lists.newArrayList();
                for(int segmentIdx=0;segmentIdx<4;segmentIdx++){
                    Integer[] intRange = new Integer[2];
                    String v = itemList.get(segmentIdx).trim();
                    if("*".equals(v)){
                        intRange[0] = 0;
                        intRange[1] = 255;
                    }else if(v.startsWith("[")&&v.endsWith("]")){
                        List<String> startEnds = Splitter.on("-").splitToList(v.substring(1,v.length()-1));
                        intRange[0] = Integer.valueOf(startEnds.get(0));
                        intRange[1] = Integer.valueOf(startEnds.get(1));
                    }else{
                        intRange[0] = Integer.valueOf(v);
                        intRange[1] = Integer.valueOf(v);
                    }
                    ipSegment.add(intRange);
                }
                for(int segmentIdx=0;segmentIdx<4;segmentIdx++){
                    ipRangeConfiguration.get(segmentIdx).add(
                            Range.closed(ipSegment.get(segmentIdx)[0], ipSegment.get(segmentIdx)[1]));
                }
            }catch (Exception e){
                logger.error("Illegal white-list ip config.key={} value={} illegal={}",key,ips,ip,e);
            }
        }
    }

    private boolean validAccessPermission(String clientIp){

        boolean accessPerm = true;
        try{
            List<String> ipSegment = Splitter.on(".").splitToList(clientIp);
            for(int i=0;i<4;i++){
                if(!ipRangeConfiguration.get(i).contains(Integer.valueOf(ipSegment.get(i)))){
                    //如果有IP段不匹配，则进行断路处理，直接判断无访问权限
                    accessPerm = false;
                    break;
                }
            }
        }catch (Exception e){
            logger.error("valid access permission error.clientIp={}",clientIp,e);
            return false;
        }
        return accessPerm;
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        String ip = getRemoteAddr(servletRequest);
        if(validAccessPermission(ip)){
            filterChain.doFilter(servletRequest,servletResponse);
        }else{
            logger.warn("Don't have authorization to access(IP-white-list intercepted). clientIp={}",ip);
            interceptHandler.handle(ip, servletResponse);
        }
    }

    private String getRemoteAddr(ServletRequest servletRequest) {
        String ip = ((HttpServletRequest) servletRequest).getHeader("X-Forwarded-For");
        if (ip == null) {
            ip = servletRequest.getRemoteAddr();
        }
        String[] ips = ip.split(",\\s");
        return ips[0];
    }

    @Override
    public void destroy() {

    }

    public interface InterceptHandler{
        void handle(String clientIp,ServletResponse servletResponse) throws IOException;
    }

}

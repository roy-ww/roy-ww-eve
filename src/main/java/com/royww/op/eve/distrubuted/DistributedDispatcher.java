package com.royww.op.eve.distrubuted;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 分布式锁
 * Created by roy.ww on 2015/12/5.
 */
public class DistributedDispatcher {

    Logger logger = LoggerFactory.getLogger(DistributedDispatcher.class);

    private static String nodePath = "/locks/distributed_lock";
    CountDownLatch countDownLatch = new CountDownLatch(1);
    //当前实例在ZK上注册的节点
    private String dispatcherNodePath = "";
    private String topic = "";

    private ZkCli4Distributed zkCli4Distributed = null;


    public DistributedDispatcher(String topic, String zkAddress)throws Exception{
        this.topic = topic;
        nodePath += "_" +topic;
        zkCli4Distributed = new ZkCli4Distributed(zkAddress);
        dispatcherNodePath = zkCli4Distributed.registerNode(nodePath);
        masterElect();
    }

    /**
     * Master 选举
     * @throws InterruptedException
     * @throws KeeperException
     */
    private void masterElect() throws InterruptedException,KeeperException{

        List<String> children = zkCli4Distributed.getChildren(nodePath);

        /*
        判断当前节点是否是MASTER节点 (节点最小的为MASTER)
         */
        if(children!=null&&children.size()>0){
            Collections.sort(children);
            if(dispatcherNodePath.equals(nodePath+"/"+children.get(0))){
                countDownLatch.countDown();
                logger.info("Being elected master.topic={}",topic);
            }else{
                logger.info("Not being elected master.topic={}",topic);
                zkCli4Distributed.exists(nodePath + "/" + children.get(0), new Watcher() {
                    @Override
                    public void process(WatchedEvent watchedEvent) {
                        if(watchedEvent.getType()== Event.EventType.NodeDeleted){
                            try {
                                masterElect();
                            }catch (Exception e){
                                logger.error("Master elect error.",e);
                            }
                        }
                    }
                });
                //如果非Master，将计数器置成1
                countDownLatch = new CountDownLatch(1);
            }
        }
    }

    /**
     * 获取分布式环境中的Master权限
     * @throws InterruptedException
     * 在同一个TOPIC中，只有一个DistributedDispatcher实例会成为MASTER，如果当前实例未被选举为Master，<br/>
     * 该方法将会堵塞，直至当前实例被选举为Master
     */
    public void waitMasterPerm() throws InterruptedException{
        countDownLatch.await();
    }

    /**
     * 获取分布式环境中的Master权限
     * @param timeout
     * @param timeUnit
     * @throws InterruptedException
     * 在同一个TOPIC中，只有一个DistributedDispatcher实例会成为MASTER，如果当前实例未被选举为Master，<br/>
     * 该方法将会堵塞，直至当前实例被选举为Master
     */
    public void waitMasterPerm(long timeout,TimeUnit timeUnit) throws InterruptedException{
        countDownLatch.await(timeout,timeUnit);
    }

    /**
     * 释放Master控制权限
     */
    public void releaseMasterPerm()throws InterruptedException,KeeperException{
        if(countDownLatch.getCount()==0){
            logger.info("Release master permission.topic={}",topic);
            countDownLatch =  new CountDownLatch(1);
            zkCli4Distributed.delete(dispatcherNodePath);
            dispatcherNodePath = zkCli4Distributed.registerNode(nodePath);
            masterElect();
        }
    }

    /**
     * 得到当前实例所处的分布式状态
     * @return DistributedState index 当前实例的排序，0表示第一个实例 total 当前实例总数
     */
    public DistributedState getState() throws InterruptedException,KeeperException{
        List<String> children = zkCli4Distributed.getChildren(nodePath);
        Collections.sort(children);
        String nodeName = dispatcherNodePath.substring(nodePath.length()+1);
        return new DistributedState(children.indexOf(nodeName),children.size());
    }

    /**
     * 判断当前实例是否是MASTER
     * @return
     */
    public boolean isMaster() {
        if(countDownLatch.getCount()>0){
            return false;
        }
        return true;
    }

    /**
     * 分布式状态
     */
    public class DistributedState{
        public DistributedState(int index,int total){
            this.index = index;
            this.total = total;
        }
        int index;
        int total;
        public int getIndex() {
            return index;
        }

        public int getTotal() {
            return total;
        }
    }

    private class ZkCli4Distributed{
        Logger logger = LoggerFactory.getLogger(ZkCli4Distributed.class);
        CountDownLatch connectedLatch = new CountDownLatch(1);
        ZooKeeper zk = null;

        public ZkCli4Distributed(String zkAddress)throws IOException,InterruptedException{

            zk = new ZooKeeper(zkAddress, 1000,new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    connectedLatch.countDown();
                }
            },true);
            connectedLatch.await();
        }

        /**
         * 注册一个ZK节点
         * @param nodePath base path
         * @return 节点的路径
         * @throws InterruptedException
         * @throws KeeperException
         */
        public String  registerNode(String nodePath)throws InterruptedException,KeeperException{
            Stat lockStat = zk.exists("/locks", false);
            if(lockStat==null){
                zk.create("/locks", new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            Stat stat = zk.exists(nodePath, false);
            if(stat==null){
                zk.create(nodePath, new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            String path = zk.create(nodePath+"/node-", new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            logger.info("registered a node.path={}",path);
            return path;
        }

        /**
         * 得到某一路径下的所有子节点
         * @param nodePath
         * @return
         * @throws InterruptedException
         * @throws KeeperException
         */
        public List<String> getChildren(String nodePath)throws InterruptedException,KeeperException{
            return zk.getChildren(nodePath, null);
        }

        /**
         * 针对某一节点注册监听
         * @param nodePath
         * @param watcher
         * @throws InterruptedException
         * @throws KeeperException
         */
        public void exists(String nodePath,Watcher watcher)throws InterruptedException,KeeperException{
            zk.exists(nodePath,watcher);
        }

        /**
         * 删除节点
         * @param nodePath
         * @throws InterruptedException
         * @throws KeeperException
         */
        public void delete(String nodePath)throws InterruptedException,KeeperException{
            zk.delete(nodePath,-1);
        }

    }
}

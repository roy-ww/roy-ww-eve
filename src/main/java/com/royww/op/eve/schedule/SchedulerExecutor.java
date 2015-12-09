package com.royww.op.eve.schedule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.*;

/**
 * 任务调度器
 * Created by roy.ww on 2015/12/07.
 */
public class SchedulerExecutor {

    Logger logger = LoggerFactory.getLogger(SchedulerExecutor.class);

    private static ExecutorService executorService = null;

    private DelayQueue<Schedule> timeLineDelayQueue = new DelayQueue<Schedule>();

    public SchedulerExecutor(){
        executorService = new ThreadPoolExecutor(5, 100, 1,
                TimeUnit.HOURS, new ArrayBlockingQueue<Runnable>(10000), new ThreadPoolExecutor.AbortPolicy());
        startThreads();
    }

    /**
     * @param threadCount 线程池线程数量
     */
    public SchedulerExecutor(int threadCount){
        executorService = new ThreadPoolExecutor(5, threadCount, 1,
                TimeUnit.HOURS, new ArrayBlockingQueue<Runnable>(10000), new ThreadPoolExecutor.AbortPolicy());
        startThreads();
    }

    /**
     * 启动相关线程
     */
    private void startThreads(){
        Thread scanThread = new Thread(new SchedulerScanThread());
        scanThread.start();
        Thread monitorThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true){
                    try {
                        logger.info("SchedulerExecutor monitor.scheduleSize={}",timeLineDelayQueue.size());
                        Thread.sleep(60000l);
                    }catch (InterruptedException e){
                        logger.error("MonitorThread Interrupted Exception.",e);
                    }
                }
            }
        });
        monitorThread.start();
    }

    /**
     * 注册一个时间任务
     *
     * @param job
     */
    public void register(final Runnable job) {
        /*
        验证任务定义的合法性
         */
        Trigger triggerAnnotation = job.getClass().getAnnotation(Trigger.class);
        if (triggerAnnotation == null || triggerAnnotation.intervals() == null || triggerAnnotation.intervals().length == 0) {
            throw new IllegalArgumentException("JobDetail object illegal.Must add annotation @Interface Trigger.class");
        }
        /*
        计算任务的执行时间点，并加入到任务队列中(JOB_TIME_LINE)中
         */
        for (long interval : triggerAnnotation.intervals()) {
            timeLineDelayQueue.offer(new Schedule(interval, triggerAnnotation.timeUnit(), job));
        }

    }

    /**
     * 时间任务
     */
    private class Schedule implements Delayed {
        private long executeTime;
        private Runnable job;

        public Schedule(long intervalTime, TimeUnit timeUnit, Runnable job) {
            this.executeTime = TimeUnit.NANOSECONDS.convert(intervalTime, timeUnit) + System.nanoTime();
            this.job = job;
        }

        public Runnable getJob() {
            return job;
        }

        public long getExecuteTime() {
            return executeTime;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(executeTime - System.nanoTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            Schedule s = (Schedule) o;
            return executeTime > s.getExecuteTime() ? 1 : (executeTime < s.getExecuteTime() ? -1 : 0);
        }
    }

    /**
     * 任务扫描线程
     */
    private class SchedulerScanThread implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    Schedule s = timeLineDelayQueue.take();
                    executorService.submit(s.getJob());
                } catch (RejectedExecutionException rejectedException) {
                    logger.error("Job is rejected.", rejectedException);
                } catch (InterruptedException e) {
                    logger.error("Scheduler scan thread interrupted.", e);
                }
            }
        }
    }

    /**
     * 定时任务触发器
     * Created by roy.ww on 2015/12/07.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface Trigger {
        long[] intervals() default {};
        TimeUnit timeUnit() default TimeUnit.MINUTES;
    }
}

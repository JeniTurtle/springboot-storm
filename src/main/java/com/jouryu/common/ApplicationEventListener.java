package com.jouryu.common;

import org.apache.log4j.Logger;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.boot.context.event.ApplicationPreparedEvent;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.ContextStartedEvent;
import org.springframework.context.event.ContextStoppedEvent;

/**
 * Created by tomorrow on 18/11/26.
 */

public class ApplicationEventListener implements ApplicationListener {

    public static Logger logger = Logger.getLogger(ApplicationEventListener.class);

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        // 在这里可以监听到Spring Boot的生命周期
        if (event instanceof ApplicationEnvironmentPreparedEvent) {
            // 初始化环境变量
        } else if (event instanceof ApplicationPreparedEvent) {
            // 初始化完成
        } else if (event instanceof ContextRefreshedEvent) {
            // 应用刷新
        } else if (event instanceof ApplicationReadyEvent) {
            // 应用已启动完成
        } else if (event instanceof ContextStartedEvent) {
            // 应用启动，需要在代码动态添加监听器才可捕获
        } else if (event instanceof ContextStoppedEvent) {
            // 应用停止
        } else if (event instanceof ContextClosedEvent) {
            // 应用关闭, 杀掉python子进程
            try {
                for (Process process : CacheData.processList) {
                    process.destroy();
                }
            } catch (Exception e) {
                logger.info("python子进程终止失败!");
                logger.error(e.getMessage());
            }
        }
    }
}
package com.alibaba.otter.canal.adapter.launcher.loader;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.cloud.context.refresh.ContextRefresher;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.adapter.launcher.common.SyncSwitch;
import com.alibaba.otter.canal.adapter.launcher.config.AdapterCanalConfig;
import com.alibaba.otter.canal.adapter.launcher.config.SpringContext;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;

/**
 * 适配器启动业务类
 *
 * @author rewerma @ 2018-10-20
 * @version 1.0.0
 */
@Component
@RefreshScope
public class CanalAdapterService {

    private static final Logger logger  = LoggerFactory.getLogger(CanalAdapterService.class);

    private CanalAdapterLoader  adapterLoader;

    @Resource
    private ContextRefresher    contextRefresher;

    @Resource
    private AdapterCanalConfig  adapterCanalConfig;
    @Resource
    private Environment         env;

    // 注入bean保证优先注册
    @Resource
    private SpringContext       springContext;
    @Resource
    private SyncSwitch          syncSwitch;

    private volatile boolean    running = false;

    /**
     * 在Bean初始化完成后执行的初始化方法
     * 该方法确保在应用程序启动时，只初始化一次
     * 同步方法，防止多线程环境下多次初始化
     */
    @PostConstruct
    public synchronized void init() {
        // 如果已经初始化并正在运行，则直接返回
        if (running) {
            return;
        }
        try {
            // 刷新同步开关，确保配置是最新的
            syncSwitch.refresh();
            logger.info("## syncSwitch refreshed.");

            // 初始化并启动Canal客户端适配器
            logger.info("## start the canal client adapters.");
            adapterLoader = new CanalAdapterLoader(adapterCanalConfig);
            adapterLoader.init();

            // 设置运行状态为true，表示适配器正在运行
            running = true;
            logger.info("## the canal client adapters are running now ......");
        } catch (Exception e) {
            // 捕获异常并记录，确保适配器的启动过程中出现的异常不会导致程序崩溃
            logger.error("## something goes wrong when starting up the canal client adapters:", e);
        }
    }

    @PreDestroy
    public synchronized void destroy() {
        if (!running) {
            return;
        }
        try {
            running = false;
            logger.info("## stop the canal client adapters");

            if (adapterLoader != null) {
                adapterLoader.destroy();
                adapterLoader = null;
            }
            for (DruidDataSource druidDataSource : DatasourceConfig.DATA_SOURCES.values()) {
                try {
                    druidDataSource.close();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
            DatasourceConfig.DATA_SOURCES.clear();
        } catch (Throwable e) {
            logger.warn("## something goes wrong when stopping canal client adapters:", e);
        } finally {
            logger.info("## canal client adapters are down.");
        }
    }
}

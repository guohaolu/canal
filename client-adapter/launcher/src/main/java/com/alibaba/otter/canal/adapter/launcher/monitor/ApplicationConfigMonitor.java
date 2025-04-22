package com.alibaba.otter.canal.adapter.launcher.monitor;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.context.refresh.ContextRefresher;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.otter.canal.adapter.launcher.loader.CanalAdapterService;
import com.alibaba.otter.canal.client.adapter.support.Util;

@Component
public class ApplicationConfigMonitor {

    private static final Logger   logger = LoggerFactory.getLogger(ApplicationConfigMonitor.class);

    @Resource
    private ContextRefresher      contextRefresher;

    @Resource
    private CanalAdapterService   canalAdapterService;

    private FileAlterationMonitor fileMonitor;

    /**
     * 初始化方法，用于设置配置文件目录的监听器
     * 该方法在类实例化后调用，主要用于启动对配置目录的监控
     */
    @PostConstruct
    public void init() {
        // 获取配置文件目录路径
        File confDir = Util.getConfDirPath();
        try {
            // 创建一个文件 alteration 观察器，只监控符合特定条件的文件
            // 这里监控的是以 "application" 开头且以 "yml" 结尾的文件
            FileAlterationObserver observer = new FileAlterationObserver(confDir,
                FileFilterUtils.and(FileFilterUtils.fileFileFilter(),
                    FileFilterUtils.prefixFileFilter("application"),
                    FileFilterUtils.suffixFileFilter("yml")));

            // 创建文件监听器实例
            FileListener listener = new FileListener();

            // 将监听器添加到观察者中，以便接收文件变动事件
            observer.addListener(listener);

            // 创建文件监控器，每3000毫秒检查一次文件变动
            fileMonitor = new FileAlterationMonitor(3000, observer);

            // 启动文件监控器
            fileMonitor.start();

        } catch (Exception e) {
            // 日志记录任何启动监控过程中发生的异常
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 在Bean销毁前执行清理工作
     * <p>
     * 该方法主要用于释放资源或停止某些在应用运行期间持续运行的过程
     * 这里专注于停止文件监控器，确保在Bean销毁时，文件监控器能够被正确关闭
     * 防止资源泄露
     * <p>
     * 注：此方法标记了@PreDestroy注解，表明这是一个在Bean生命周期结束前会被调用的方法
     */
    @PreDestroy
    public void destroy() {
        try {
            // 尝试停止文件监控器
            fileMonitor.stop();
        } catch (Exception e) {
            // 如果在停止文件监控器过程中发生异常，记录错误信息
            logger.error(e.getMessage(), e);
        }
    }

    private class FileListener extends FileAlterationListenerAdaptor {

        @Override
        public void onFileChange(File file) {
            super.onFileChange(file);
            try {
                // 检查yml格式
                new Yaml().loadAs(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8), Map.class);

                canalAdapterService.destroy();

                // refresh context
                contextRefresher.refresh();

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    // ignore
                }
                canalAdapterService.init();
                logger.info("## adapter application config reloaded.");
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}

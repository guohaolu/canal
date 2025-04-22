package com.alibaba.otter.canal.adapter.launcher.common;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.stereotype.Component;

import com.alibaba.otter.canal.adapter.launcher.config.AdapterCanalConfig;
import com.alibaba.otter.canal.adapter.launcher.config.CuratorClient;
import com.alibaba.otter.canal.common.utils.BooleanMutex;

/**
 * 同步开关
 *
 * @author rewerma @ 2018-10-20
 * @version 1.0.0
 */
@Component
public class SyncSwitch {

    private static final String                    SYN_SWITCH_ZK_NODE = "/sync-switch/";

    private static final Map<String, BooleanMutex> LOCAL_LOCK         = new ConcurrentHashMap<>();

    private static final Map<String, BooleanMutex> DISTRIBUTED_LOCK   = new ConcurrentHashMap<>();

    private Mode                                   mode               = Mode.LOCAL;

    @Resource
    private AdapterCanalConfig                     adapterCanalConfig;
    @Resource
    private CuratorClient                          curatorClient;

    @PostConstruct
    public void init() {
        CuratorFramework curator = curatorClient.getCurator();
        if (curator != null) {
            mode = Mode.DISTRIBUTED;
            DISTRIBUTED_LOCK.clear();
            for (String destination : adapterCanalConfig.DESTINATIONS) {
                // 对应每个destination注册锁
                BooleanMutex mutex = new BooleanMutex(true);
                initMutex(curator, destination, mutex);
                DISTRIBUTED_LOCK.put(destination, mutex);
                startListen(destination, mutex);
            }
        } else {
            mode = Mode.LOCAL;
            LOCAL_LOCK.clear();
            for (String destination : adapterCanalConfig.DESTINATIONS) {
                // 对应每个destination注册锁
                LOCAL_LOCK.put(destination, new BooleanMutex(true));
            }
        }
    }

    /**
     * 刷新配置信息
     * 此方法用于同步更新配置信息，确保在分布式或多线程环境下安全地更新和监听配置变化
     */
    public synchronized void refresh() {
        // 遍历所有目的地配置
        for (String destination : adapterCanalConfig.DESTINATIONS) {
            BooleanMutex booleanMutex;
            // 根据模式选择不同的锁策略
            if (mode == Mode.DISTRIBUTED) {
                // 在分布式模式下，获取Curator客户端实例
                CuratorFramework curator = curatorClient.getCurator();
                // 尝试从分布式锁映射中获取当前目的地的锁对象
                booleanMutex = DISTRIBUTED_LOCK.get(destination);
                // 如果当前目的地的锁对象不存在，说明尚未初始化，进行初始化操作
                if (booleanMutex == null) {
                    // 创建一个新的布尔互斥锁，并设置为可用状态
                    BooleanMutex mutex = new BooleanMutex(true);
                    // 初始化分布式锁
                    initMutex(curator, destination, mutex);
                    // 将新创建的锁对象添加到分布式锁映射中
                    DISTRIBUTED_LOCK.put(destination, mutex);
                    // 开始监听当前目的地的配置变化，并传入锁对象以协调监听过程
                    startListen(destination, mutex);
                }
            } else {
                // 在非分布式模式下，直接从本地锁映射中获取当前目的地的锁对象
                booleanMutex = LOCAL_LOCK.get(destination);
                // 如果当前目的地的锁对象不存在，说明尚未初始化，进行初始化操作
                if (booleanMutex == null) {
                    // 创建一个新的布尔互斥锁并添加到本地锁映射中
                    LOCAL_LOCK.put(destination, new BooleanMutex(true));
                }
            }
        }
    }

    @SuppressWarnings("resource")
    private synchronized void startListen(String destination, BooleanMutex mutex) {
        try {
            String path = SYN_SWITCH_ZK_NODE + destination;
            CuratorFramework curator = curatorClient.getCurator();
            NodeCache nodeCache = new NodeCache(curator, path);
            nodeCache.start();
            nodeCache.getListenable().addListener(() -> initMutex(curator, destination, mutex));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private synchronized void initMutex(CuratorFramework curator, String destination, BooleanMutex mutex) {
        try {
            String path = SYN_SWITCH_ZK_NODE + destination;
            Stat stat = curator.checkExists().forPath(path);
            if (stat == null) {
                if (!mutex.state()) {
                    mutex.set(true);
                }
            } else {
                String data = new String(curator.getData().forPath(path), StandardCharsets.UTF_8);
                if ("on".equals(data)) {
                    if (!mutex.state()) {
                        mutex.set(true);
                    }
                } else {
                    if (mutex.state()) {
                        mutex.set(false);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public synchronized void off(String destination) {
        if (mode == Mode.LOCAL) {
            BooleanMutex mutex = LOCAL_LOCK.get(destination);
            if (mutex != null && mutex.state()) {
                mutex.set(false);
            }
        } else {
            try {
                String path = SYN_SWITCH_ZK_NODE + destination;
                try {
                    curatorClient.getCurator()
                        .create()
                        .creatingParentContainersIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(path, "off".getBytes(StandardCharsets.UTF_8));
                } catch (Exception e) {
                    curatorClient.getCurator().setData().forPath(path, "off".getBytes(StandardCharsets.UTF_8));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public synchronized void on(String destination) {
        if (mode == Mode.LOCAL) {
            BooleanMutex mutex = LOCAL_LOCK.get(destination);
            if (mutex != null && !mutex.state()) {
                mutex.set(true);
            }
        } else {
            try {
                String path = SYN_SWITCH_ZK_NODE + destination;
                try {
                    curatorClient.getCurator()
                        .create()
                        .creatingParentContainersIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(path, "on".getBytes(StandardCharsets.UTF_8));
                } catch (Exception e) {
                    curatorClient.getCurator().setData().forPath(path, "on".getBytes(StandardCharsets.UTF_8));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public synchronized void release(String destination) {
        if (mode == Mode.LOCAL) {
            BooleanMutex mutex = LOCAL_LOCK.get(destination);
            if (mutex != null && !mutex.state()) {
                mutex.set(true);
            }
        }
        if (mode == Mode.DISTRIBUTED) {
            BooleanMutex mutex = DISTRIBUTED_LOCK.get(destination);
            if (mutex != null && !mutex.state()) {
                mutex.set(true);
            }
        }
    }

    public boolean status(String destination) {
        if (mode == Mode.LOCAL) {
            BooleanMutex mutex = LOCAL_LOCK.get(destination);
            if (mutex != null) {
                return mutex.state();
            } else {
                return false;
            }
        } else {
            BooleanMutex mutex = DISTRIBUTED_LOCK.get(destination);
            if (mutex != null) {
                return mutex.state();
            } else {
                return false;
            }
        }
    }

    public void get(String destination) throws InterruptedException {
        if (mode == Mode.LOCAL) {
            BooleanMutex mutex = LOCAL_LOCK.get(destination);
            if (mutex != null) {
                mutex.get();
            }
        } else {
            BooleanMutex mutex = DISTRIBUTED_LOCK.get(destination);
            if (mutex != null) {
                mutex.get();
            }
        }
    }

    public void get(String destination, long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        if (mode == Mode.LOCAL) {
            BooleanMutex mutex = LOCAL_LOCK.get(destination);
            if (mutex != null) {
                mutex.get(timeout, unit);
            }
        } else {
            BooleanMutex mutex = DISTRIBUTED_LOCK.get(destination);
            if (mutex != null) {
                mutex.get(timeout, unit);
            }
        }
    }

}

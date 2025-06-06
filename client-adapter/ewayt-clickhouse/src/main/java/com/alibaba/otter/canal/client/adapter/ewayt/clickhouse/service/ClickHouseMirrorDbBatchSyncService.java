package com.alibaba.otter.canal.client.adapter.ewayt.clickhouse.service;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter.Feature;
import com.alibaba.otter.canal.client.adapter.ewayt.clickhouse.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.ewayt.clickhouse.config.MirrorDbConfig;
import com.alibaba.otter.canal.client.adapter.ewayt.clickhouse.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.Dml;

/**
 * CLICKHOUSE镜像库同步操作业务
 *
 * @author: Xander
 * @date: Created in 2023/11/10 22:23
 * @email: zhrunxin33@gmail.com
 * @version 1.1.8
 */
public class ClickHouseMirrorDbBatchSyncService {

    private static final Logger         logger = LoggerFactory.getLogger(ClickHouseMirrorDbBatchSyncService.class);

    // 镜像库配置
    private Map<String, MirrorDbConfig> mirrorDbConfigCache;
    private DruidDataSource             dataSource;
    // clickhouseSyncService代理
    private ClickHouseBatchSyncService  clickHouseBatchSyncService;

    public ClickHouseMirrorDbBatchSyncService(Map<String, MirrorDbConfig> mirrorDbConfigCache,
                                              DruidDataSource dataSource, Integer threads, Integer batchSize,
                                              Long scheduleTime, Map<String, Map<String, Integer>> columnsTypeCache,
                                              boolean skipDupException){
        this.mirrorDbConfigCache = mirrorDbConfigCache;
        this.dataSource = dataSource;
        this.clickHouseBatchSyncService = new ClickHouseBatchSyncService(dataSource,
            threads,
            batchSize,
            scheduleTime,
            columnsTypeCache,
            skipDupException);
    }

    /**
     * 批量同步方法
     *
     * @param dmls 批量 DML，包含DDL
     */
    public void sync(List<Dml> dmls) {
        List<Dml> dmlList = new ArrayList<>();
        for (Dml dml : dmls) {
            String destination = StringUtils.trimToEmpty(dml.getDestination());
            String database = dml.getDatabase();
            MirrorDbConfig mirrorDbConfig = mirrorDbConfigCache.get(destination + "." + database);
            if (mirrorDbConfig == null) {
                continue;
            }
            if (mirrorDbConfig.getMappingConfig() == null) {
                continue;
            }
            if (dml.getGroupId() != null && StringUtils.isNotEmpty(mirrorDbConfig.getMappingConfig().getGroupId())) {
                if (!mirrorDbConfig.getMappingConfig().getGroupId().equals(dml.getGroupId())) {
                    continue; // 如果groupId不匹配则过滤
                }
            }

            if (dml.getIsDdl() != null && dml.getIsDdl() && StringUtils.isNotEmpty(dml.getSql())) {
                // 确保执行DDL前DML已执行完
                syncDml(dmlList);
                dmlList.clear();

                // DDL
                if (logger.isDebugEnabled()) {
                    logger.debug("DDL: {}", JSON.toJSONString(dml, Feature.WriteNulls));
                }
                executeDdl(mirrorDbConfig, dml);
                clickHouseBatchSyncService.getColumnsTypeCache()
                    .remove(destination + "." + database + "." + dml.getTable());
                mirrorDbConfig.getTableConfig().remove(dml.getTable()); // 删除对应库表配置
            } else {
                // DML
                initMappingConfig(dml.getTable(), mirrorDbConfig.getMappingConfig(), mirrorDbConfig, dml);
                dmlList.add(dml);
            }
        }
        syncDml(dmlList);
    }

    /**
     * 批量同步Dml
     *
     * @param dmlList Dml列表，不包含DDL
     */
    private void syncDml(List<Dml> dmlList) {
        if (dmlList == null || dmlList.isEmpty()) {
            return;
        }
        clickHouseBatchSyncService.sync(dmlList, dml -> {
            MirrorDbConfig mirrorDbConfig = mirrorDbConfigCache.get(dml.getDestination() + "." + dml.getDatabase());
            if (mirrorDbConfig == null) {
                return false;
            }
            String table = dml.getTable();
            MappingConfig config = mirrorDbConfig.getTableConfig().get(table);

            if (config == null) {
                return false;
            }
            clickHouseBatchSyncService.appendDmlBufferPartition(config, dml);
            return true;
        });
    }

    /**
     * 初始化表配置
     *
     * @param key 配置key: destination.database.table
     * @param baseConfigMap db sync config
     * @param dml DML
     */
    private void initMappingConfig(String key, MappingConfig baseConfigMap, MirrorDbConfig mirrorDbConfig, Dml dml) {
        MappingConfig mappingConfig = mirrorDbConfig.getTableConfig().get(key);
        if (mappingConfig == null) {
            // 构造表配置
            mappingConfig = new MappingConfig();
            mappingConfig.setDataSourceKey(baseConfigMap.getDataSourceKey());
            mappingConfig.setDestination(baseConfigMap.getDestination());
            mappingConfig.setGroupId(baseConfigMap.getGroupId());
            mappingConfig.setOuterAdapterKey(baseConfigMap.getOuterAdapterKey());
            mappingConfig.setConcurrent(baseConfigMap.getConcurrent());
            MappingConfig.DbMapping dbMapping = new MappingConfig.DbMapping();
            mappingConfig.setDbMapping(dbMapping);
            dbMapping.setDatabase(dml.getDatabase());
            dbMapping.setTable(dml.getTable());
            dbMapping.setTargetDb(dml.getDatabase());
            dbMapping.setTargetTable(dml.getTable());
            dbMapping.setMapAll(true);
            List<String> pkNames = dml.getPkNames();
            Map<String, String> pkMapping = new LinkedHashMap<>();
            pkNames.forEach(pkName -> pkMapping.put(pkName, pkName));
            dbMapping.setTargetPk(pkMapping);

            mirrorDbConfig.getTableConfig().put(key, mappingConfig);
        }
    }

    /**
     * DDL 操作
     *
     * @param ddl DDL
     */
    private void executeDdl(MirrorDbConfig mirrorDbConfig, Dml ddl) {
        try (Connection conn = dataSource.getConnection(); Statement statement = conn.createStatement()) {
            // 替换反引号
            String sql = ddl.getSql();
            String backtick = SyncUtil.getBacktickByDbType(dataSource.getDbType());
            if (!"`".equals(backtick)) {
                sql = sql.replaceAll("`", backtick);
            }
            statement.execute(sql);
            // 移除对应配置
            mirrorDbConfig.getTableConfig().remove(ddl.getTable());
            if (logger.isTraceEnabled()) {
                logger.trace("Execute DDL sql: {} for database: {}", ddl.getSql(), ddl.getDatabase());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

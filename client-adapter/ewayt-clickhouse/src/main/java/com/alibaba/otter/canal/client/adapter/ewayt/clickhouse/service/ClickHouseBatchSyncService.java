package com.alibaba.otter.canal.client.adapter.ewayt.clickhouse.service;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.alibaba.otter.canal.client.adapter.ewayt.clickhouse.EwaytClickHouseAdapter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter.Feature;
import com.alibaba.otter.canal.client.adapter.ewayt.clickhouse.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.ewayt.clickhouse.config.MappingConfig.DbMapping;
import com.alibaba.otter.canal.client.adapter.ewayt.clickhouse.support.BatchExecutor;
import com.alibaba.otter.canal.client.adapter.ewayt.clickhouse.support.SingleDml;
import com.alibaba.otter.canal.client.adapter.ewayt.clickhouse.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.Util;

/**
 * ClickHouse batch synchronize
 *
 * @author: Xander
 * @date: Created in 2023/11/10 22:23
 * @email: zhrunxin33@gmail.com
 * @version 1.1.8
 */
public class ClickHouseBatchSyncService {

    private static final Logger                         logger  = LoggerFactory.getLogger(ClickHouseBatchSyncService.class);

    private DruidDataSource                             dataSource;

    private Map<String, Map<String, Integer>>           columnsTypeCache;     // Cache of instance.schema.table -> <columnName, jdbcType>

    private Map<MappingConfig, List<SingleDml>>[]       bufferPools;          // Data buffer pool store sync data, List<Dml> dispersed as arrays according to hash value

    private BatchExecutor[]                             batchExecutors;       // Batch Executor

    private BatchExecutor                               alterExecutors;       // Alter Single Executor(update/delete/truncate)

    private ExecutorService[]                           executorThreads;      // Be initialized once

    private ScheduledExecutorService[]                  scheduledExecutors;

    private int                                         threads = 3;          // Default parallel thread count
    private int                                         batchSize = 1000;
    private long                                        scheduleTime = 10;
    private boolean                                     skipDupException;

    public Map<String, Map<String, Integer>> getColumnsTypeCache() {
        return columnsTypeCache;
    }

    public ClickHouseBatchSyncService(DruidDataSource dataSource, Integer threads, Integer batchSize, Long scheduleTime, boolean skipDupException){
        this(dataSource, threads, batchSize, scheduleTime, new ConcurrentHashMap<>(), skipDupException);
    }

    @SuppressWarnings("unchecked")
    public ClickHouseBatchSyncService(DruidDataSource dataSource, Integer threads, Integer batchSize, Long scheduleTime, Map<String, Map<String, Integer>> columnsTypeCache,
                                      boolean skipDupException){
        this.dataSource = dataSource;
        this.columnsTypeCache = columnsTypeCache;
        this.skipDupException = skipDupException;
        try {
            if (threads != null) {
                this.threads = threads;
            }
            if (batchSize != null) {
                this.batchSize = batchSize;
            }
            if (scheduleTime != null) {
                this.scheduleTime = scheduleTime;
            }
            this.alterExecutors = new BatchExecutor(dataSource);
            this.bufferPools = new ConcurrentHashMap[this.threads];
            this.batchExecutors = new BatchExecutor[this.threads];
            this.executorThreads = new ExecutorService[this.threads];
            this.scheduledExecutors = new ScheduledExecutorService[this.threads];
            for (int i = 0; i < this.threads; i++) {
                bufferPools[i] = new ConcurrentHashMap<>();
                batchExecutors[i] = new BatchExecutor(dataSource);
                executorThreads[i] = Executors.newSingleThreadExecutor();
                scheduledExecutors[i] = Executors.newSingleThreadScheduledExecutor();
            }
            scheduleBatchSync();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Timing-driven event
     * start schedule batch sync threadPool
     */
    private void scheduleBatchSync() {
        for (int i = 0; i < scheduledExecutors.length; i++) {
            int index = i;
            scheduledExecutors[i].scheduleAtFixedRate(()->{
                List<Future<Boolean>> futures = new ArrayList<>();
                for (MappingConfig mappingConfig : bufferPools[index].keySet()) {
                    List<SingleDml> dmls = bufferPools[index].get(mappingConfig);
                    if (dmls == null || dmls.isEmpty()) {
                        return;
                    }
                    List<SingleDml> tempDmls;
                    synchronized (dmls) {
                        tempDmls = new ArrayList<>(dmls);
                        dmls.clear();
                    }
                    futures.add(executorThreads[index].submit(()->{
                        try {
                            insert(batchExecutors[index], mappingConfig, tempDmls);
                            batchExecutors[index].commit();
                            return true;
                        } catch (Exception e){
                            batchExecutors[index].rollback();
                            throw new RuntimeException(e);
                        }
                    }));
                }
            }, 0, scheduleTime, TimeUnit.SECONDS);
        }
        logger.info("Schedule batch executors has started successfully!");
    }

    /**
     * 批量同步回调
     *
     * @param dmls 批量 DML
     * @param function 回调方法
     */
    public void sync(List<Dml> dmls, Function<Dml, Boolean> function) {
        boolean toExecute = false;
        for (Dml dml : dmls) {
            if (!toExecute) {
                toExecute = function.apply(dml);
            } else {
                function.apply(dml);
            }
        }
    }

    /**
     * 同步数据到目标数据库
     *
     * @param mappingConfig 映射配置，包含目标数据库的配置信息
     * @param dmls 待处理的数据操作语言（DML）列表
     * @param envProperties 环境属性，包含运行环境的配置信息
     */
    public void sync(Map<String, Map<String, MappingConfig>> mappingConfig, List<Dml> dmls, Properties envProperties) {
        // 调用重载的sync方法，传入DML列表和处理逻辑
        sync(dmls, dml -> {
            // 判断是否为DDL语句
            if (dml.getIsDdl() != null && dml.getIsDdl() && StringUtils.isNotEmpty(dml.getSql())) {
                // DDL语句出现时，移除缓存数据
                columnsTypeCache.remove(dml.getDestination() + "." + dml.getDatabase() + "." + dml.getTable());
                return false;
            } else {
                // DML
                // 获取目标、组、数据库和表的信息，并进行空字符串处理
                String destination = StringUtils.trimToEmpty(dml.getDestination());
                String groupId = StringUtils.trimToEmpty(dml.getGroupId());
                String database = dml.getDatabase();
                String table = dml.getTable();
                Map<String, MappingConfig> configMap;
                // 根据环境属性中的配置模式获取对应的映射配置
                if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                    configMap = mappingConfig.get(destination + "-" + groupId + "_" + database + "-" + table);
                } else {
                    configMap = mappingConfig.get(destination + "_" + database + "-" + table);
                }

                // 如果没有找到对应的映射配置，则不处理该DML
                if (configMap == null) {
                    return false;
                }

                // 如果映射配置为空，则不处理该DML
                if (configMap.values().isEmpty()) {
                    return false;
                }

                // 遍历映射配置，分发DML到目标数据库
                for (MappingConfig config : configMap.values()) {
                    distributeDml(config, dml);
                }
                return true;
            }
        });
    }

    /**
     * 根据配置信息分发DML（数据操作语言）到相应的处理方法
     * 该方法主要用于处理插入、更新、删除和清空表的操作
     *
     * @param config 映射配置对象，包含数据库映射和执行策略等信息
     * @param dml 数据操作语言对象，代表待执行的数据库操作
     */
    private void distributeDml(MappingConfig config, Dml dml) {
        // 检查配置对象是否非空，为空则直接返回
        if (config != null) {
            try {
                // 获取DML的类型，如INSERT、UPDATE、DELETE或TRUNCATE
                String type = dml.getType();
                // 如果类型为空，则直接返回
                if (type == null) return;

                // 根据DML类型进行不同的处理
                if ("INSERT".equalsIgnoreCase(type) || "UPDATE".equalsIgnoreCase(type) || "DELETE".equalsIgnoreCase(type)) {
                    // 如果是插入操作，调用appendDmlBufferPartition方法处理
                    appendDmlBufferPartition(config, dml);
                } else {
                    // 非插入操作，获取数据库映射的大小写敏感配置
                    boolean caseInsensitive = config.getDbMapping().isCaseInsensitive();
                    // 将DML拆分为单个DML操作列表，根据数据库映射和大小写敏感配置
                    List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml, caseInsensitive);

                    // 根据DML类型进行不同的处理
                    if ("UPDATE".equalsIgnoreCase(type)) {
                        // 如果是更新操作，遍历每个单个DML并调用update方法处理
                        for (SingleDml singleDml : singleDmls) {
                            update(alterExecutors, config, singleDml);
                        }
                    } else if ("DELETE".equalsIgnoreCase(type)) {
                        // 如果是删除操作，遍历每个单个DML并调用delete方法处理
                        for (SingleDml singleDml : singleDmls) {
                            delete(alterExecutors, config, singleDml);
                        }
                    } else if ("TRUNCATE".equalsIgnoreCase(type)) {
                        // 如果是清空表操作，调用truncate方法处理
                        truncate(alterExecutors, config);
                    }
                }
                // 如果日志级别为调试模式，打印DML的详细信息
                if (logger.isDebugEnabled()) {
                    logger.debug("DML: {}", JSON.toJSONString(dml, Feature.WriteNulls));
                }
            } catch (SQLException e) {
                // 捕获SQL异常，并将其包装为运行时异常抛出
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 将DML语句添加到缓冲区 partitions 中
     * 根据数据库映射和DML语句生成哈希值，以确定数据应存储在哪个缓冲区 partition 中
     * 当缓冲区中的数据量达到预设阈值时，将数据同步到ClickHouse
     *
     * @param config 映射配置，包含数据库映射信息和并发配置
     * @param dml 待处理的DML语句
     */
    public void appendDmlBufferPartition(MappingConfig config, Dml dml) {
        // 获取数据库映射的大小写敏感配置
        boolean caseInsensitive = config.getDbMapping().isCaseInsensitive();
        // 将DML语句拆分为单个DML语句列表，以便于后续处理
        List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml, caseInsensitive);

        // 遍历每个单个DML语句
        for (SingleDml singleDml : singleDmls) {
            // 计算哈希值以确定缓冲区 partition
            int hash = mappingHash(config.getDbMapping());
            // 如果配置为非并发模式，则将哈希值设为0，以确保所有数据进入同一个缓冲区
            if (!config.getConcurrent()) {
                hash = 0;
            }
            // 获取或创建缓冲区中的DML语句列表
            List<SingleDml> dmls = bufferPools[hash].computeIfAbsent(config, k -> new ArrayList<>());
            // 同步块，以确保线程安全
            synchronized (dmls) {
                // 将单个DML语句添加到缓冲区中
                dmls.add(singleDml);
                // 记录日志，包含添加的数据的ID
                logger.info("Append one data into pool, id {}", singleDml.getData().get("id"));
            }
            // Check the size of the List, achieve when it reaches the maximum value
            if (dmls.size() >= batchSize) syncToClickHouse(config, hash);
        }
    }

    /**
     * sync when size of list{@link #bufferPools } reaches the maximum value
     *
     * @param config key
     * @param index parallel thread index
     */
    private void syncToClickHouse(MappingConfig config, int index) {
        List<SingleDml> dmls = bufferPools[index].get(config);
        logger.info("schema:{} table:{} reaches the maximum value, ready to synchronize, size {}", config.getDbMapping().getDatabase(), config.getDbMapping().getTable(), dmls.size());
        if (dmls ==null || dmls.isEmpty()) {
            return;
        }
        List<SingleDml> tempDmls;
        synchronized (dmls) {
            tempDmls = new ArrayList<>(dmls);
            dmls.clear();
        }
        executorThreads[index].submit(() -> {
            try {
                insert(batchExecutors[index], config, tempDmls);
                batchExecutors[index].commit();
                return true;
            } catch (Exception e) {
                batchExecutors[index].rollback();
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * 执行插入操作
     *
     * @param batchExecutor 批量执行器，用于执行批量插入操作
     * @param config 映射配置，包含数据库映射信息
     * @param dmls 待插入的DML列表，包含多个SingleDml对象
     * @throws SQLException 如果插入过程中发生SQL异常
     */
    private void insert(BatchExecutor batchExecutor, MappingConfig config, List<SingleDml> dmls) throws SQLException {
        // 检查DML列表是否为空或为null，如果是，则直接返回
        if (dmls == null || dmls.isEmpty()) {
            return;
        }

        // 过滤出数据不为空或非空的DML
        List<SingleDml> clearDmls = dmls.stream().filter(e -> e.getData() != null && !e.getData().isEmpty()).collect(Collectors.toList());
        // 如果过滤后的DML列表为空或为null，则直接返回
        if (clearDmls == null || clearDmls.isEmpty()) {
            return;
        }

        // 获取数据库映射配置
        DbMapping dbMapping = config.getDbMapping();
        // 获取数据库类型对应的反引号字符
        String backtick = SyncUtil.getBacktickByDbType(dataSource.getDbType());
        // 获取列名映射
        Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, clearDmls.get(0).getData());

        // 新增版本号和逻辑删除
        columnsMap.put("_version", "_version");
        columnsMap.put("_is_deleted", "_is_deleted");

        // 构建插入SQL语句的StringBuilder对象
        StringBuilder insertSql = new StringBuilder();
        // 拼接插入SQL的表名部分
        insertSql.append("INSERT INTO ").append(SyncUtil.getDbTableName(dbMapping, dataSource.getDbType())).append(" (");

        // 拼接列名部分
        columnsMap.forEach((targetColumnName, srcColumnName) -> insertSql.append(backtick)
            .append(targetColumnName)
            .append(backtick)
            .append(","));
        // 移除最后一个逗号
        int len = insertSql.length();
        insertSql.delete(len - 1, len).append(") VALUES (");
        // 拼接占位符部分
        int mapLen = columnsMap.size();
        for (int i = 0; i < mapLen; i++) {
            insertSql.append("?,");
        }
        // 移除最后一个逗号
        len = insertSql.length();
        insertSql.delete(len - 1, len).append(")");

        // 获取目标列的数据类型
        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);

        // 准备存储插入值的列表
        List<List<Map<String, ?>>> values = new ArrayList<>();
        // 标志变量，用于优化循环
        boolean flag = false;
        // 遍历列名映射，准备插入数据
        for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            // 如果源列名为空，则进行清理操作
            if (srcColumnName == null) {
                srcColumnName = Util.cleanColumn(targetColumnName);
            }

            // 获取目标列的数据类型
            Integer type = ctype.get(Util.cleanColumn(targetColumnName).toLowerCase());
            // 如果列类型未找到，则抛出异常
            if (type == null) {
                throw new RuntimeException("Target column: " + targetColumnName + " not matched");
            }
            // 遍历DML列表，设置插入值
            for (int i = 0; i < clearDmls.size(); i++) {
                Map<String, Object> dmlData = clearDmls.get(i).getData();
                List<Map<String, ?>> item;
                // 根据flag决定是否创建新列表项
                if (flag == false) {
                    item = new ArrayList<>();
                    values.add(item);
                } else {
                    item = values.get(i);
                }
                // 获取列值并设置到列表项中
                Object value = dmlData.get(srcColumnName);
                BatchExecutor.setValue(item, type, value);
            }
            // 设置flag为true，表示已经处理过至少一列
            flag = true;
        }

        // 执行批量插入操作
        try {
            batchExecutor.batchExecute(insertSql.toString(), values);
        } catch (SQLException e) {
            // 处理主键冲突的情况
            if (skipDupException
                && (e.getMessage().contains("Duplicate entry") || e.getMessage().startsWith("ORA-00001:"))) {
                // ignore
                // TODO 增加更多关系数据库的主键冲突的错误码
            } else {
                throw e;
            }
        }
        // 记录日志
        if (logger.isTraceEnabled()) {
            logger.trace("Insert into target table, sql: {}", insertSql);
        }

    }

    /**
     * 更新操作
     *
     * @param config 配置项
     * @param dml DML数据
     */
    private void update(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) throws SQLException {
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        Map<String, Object> old = dml.getOld();
        if (old == null || old.isEmpty()) {
            return;
        }

        DbMapping dbMapping = config.getDbMapping();
        String backtick = SyncUtil.getBacktickByDbType(dataSource.getDbType());
        Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, data);

        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);

        StringBuilder updateSql = new StringBuilder();
        updateSql.append("ALTER TABLE ").append(SyncUtil.getDbTableName(dbMapping, dataSource.getDbType())).append(" UPDATE ");
        List<Map<String, ?>> values = new ArrayList<>();
        boolean hasMatched = false;
        for (String srcColumnName : old.keySet()) {
            List<String> targetColumnNames = new ArrayList<>();
            columnsMap.forEach((targetColumn, srcColumn) -> {
                if (srcColumnName.equalsIgnoreCase(srcColumn)) {
                    targetColumnNames.add(targetColumn);
                }
            });
            if (!targetColumnNames.isEmpty()) {
                hasMatched = true;
                for (String targetColumnName : targetColumnNames) {
                    updateSql.append(backtick).append(targetColumnName).append(backtick).append("=?, ");
                    Integer type = ctype.get(Util.cleanColumn(targetColumnName).toLowerCase());
                    if (type == null) {
                        throw new RuntimeException("Target column: " + targetColumnName + " not matched");
                    }
                    BatchExecutor.setValue(values, type, data.get(srcColumnName));
                }
            }
        }
        if (!hasMatched) {
            logger.warn("Did not matched any columns to update ");
            return;
        }
        int len = updateSql.length();
        updateSql.delete(len - 2, len).append(" WHERE ");

        // 拼接主键
        appendCondition(dbMapping, updateSql, ctype, values, data, old);
        batchExecutor.execute(updateSql.toString(), values);
        if (logger.isTraceEnabled()) {
            logger.trace("Alter update target table, sql: {}", updateSql);
        }
    }

    /**
     * 删除操作
     *
     * @param config
     * @param dml
     */
    private void delete(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) throws SQLException {
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        DbMapping dbMapping = config.getDbMapping();
        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);

        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(SyncUtil.getDbTableName(dbMapping, dataSource.getDbType())).append(" DELETE WHERE ");

        List<Map<String, ?>> values = new ArrayList<>();
        // 拼接主键
        appendCondition(dbMapping, sql, ctype, values, data);
        batchExecutor.execute(sql.toString(), values);
        if (logger.isTraceEnabled()) {
            logger.trace("Alter delete from target table, sql: {}", sql);
        }
    }

    /**
     * truncate操作
     *
     * @param config
     */
    private void truncate(BatchExecutor batchExecutor, MappingConfig config) throws SQLException {
        DbMapping dbMapping = config.getDbMapping();
        StringBuilder sql = new StringBuilder();
        sql.append("TRUNCATE TABLE ").append(SyncUtil.getDbTableName(dbMapping, dataSource.getDbType()));
        batchExecutor.execute(sql.toString(), new ArrayList<>());
        if (logger.isTraceEnabled()) {
            logger.trace("Truncate target table, sql: {}", sql);
        }
    }

    /**
     * 获取目标字段类型
     *
     * @param conn sql connection
     * @param config 映射配置
     * @return 字段sqlType
     */
    private Map<String, Integer> getTargetColumnType(Connection conn, MappingConfig config) {
        DbMapping dbMapping = config.getDbMapping();
        String cacheKey = config.getDestination() + "." + dbMapping.getDatabase() + "." + dbMapping.getTable();
        Map<String, Integer> columnType = columnsTypeCache.get(cacheKey);
        if (columnType == null) {
            synchronized (ClickHouseBatchSyncService.class) {
                columnType = columnsTypeCache.get(cacheKey);
                if (columnType == null) {
                    columnType = new LinkedHashMap<>();
                    final Map<String, Integer> columnTypeTmp = columnType;
                    String sql = "SELECT * FROM " + SyncUtil.getDbTableName(dbMapping, dataSource.getDbType()) + " WHERE 1=2";
                    Util.sqlRS(conn, sql, rs -> {
                        try {
                            ResultSetMetaData rsd = rs.getMetaData();
                            int columnCount = rsd.getColumnCount();
                            for (int i = 1; i <= columnCount; i++) {
                                int colType = rsd.getColumnType(i);
                                // 修复year类型作为date处理时的data truncated问题
                                if ("YEAR".equals(rsd.getColumnTypeName(i))) {
                                    colType = Types.VARCHAR;
                                }
                                columnTypeTmp.put(rsd.getColumnName(i).toLowerCase(), colType);
                            }
                            columnsTypeCache.put(cacheKey, columnTypeTmp);
                        } catch (SQLException e) {
                            logger.error(e.getMessage(), e);
                        }
                    });
                }
            }
        }
        return columnType;
    }

    /**
     * 拼接主键 where条件
     */
    private void appendCondition(MappingConfig.DbMapping dbMapping, StringBuilder sql, Map<String, Integer> ctype,
                                 List<Map<String, ?>> values, Map<String, Object> d) {
        appendCondition(dbMapping, sql, ctype, values, d, null);
    }

    private void appendCondition(MappingConfig.DbMapping dbMapping, StringBuilder sql, Map<String, Integer> ctype,
                                 List<Map<String, ?>> values, Map<String, Object> d, Map<String, Object> o) {
        String backtick = SyncUtil.getBacktickByDbType(dataSource.getDbType());

        // 拼接主键
        for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = Util.cleanColumn(targetColumnName);
            }
            sql.append(backtick).append(targetColumnName).append(backtick).append("=? AND ");
            Integer type = ctype.get(Util.cleanColumn(targetColumnName).toLowerCase());
            if (type == null) {
                throw new RuntimeException("Target column: " + targetColumnName + " not matched");
            }
            // 如果有修改主键的情况
            if (o != null && o.containsKey(srcColumnName)) {
                BatchExecutor.setValue(values, type, o.get(srcColumnName));
            } else {
                BatchExecutor.setValue(values, type, d.get(srcColumnName));
            }
        }
        int len = sql.length();
        sql.delete(len - 4, len);
    }

    /**
     * make sure the same table in one index
     *
     * @param dbMapping
     * @return
     */
    private int mappingHash(MappingConfig.DbMapping dbMapping) {
        int hash = dbMapping.getDatabase().toLowerCase().hashCode() + dbMapping.getTable().toLowerCase().hashCode();
        hash = Math.abs(hash) % threads;
        return Math.abs(hash);
    }

    public void close() {
        for (int i = 0; i < threads; i++) {
            executorThreads[i].shutdown();
        }
    }
}

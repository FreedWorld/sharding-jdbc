package com.dangdang.ddframe.rdb.sharding.config.yaml.hash.api;

import com.dangdang.ddframe.rdb.sharding.config.common.api.ShardingRuleBuilder;
import com.dangdang.ddframe.rdb.sharding.config.common.api.config.StrategyConfig;
import com.dangdang.ddframe.rdb.sharding.config.common.api.config.TableRuleConfig;
import com.dangdang.ddframe.rdb.sharding.config.yaml.hash.algorithm.HashAlgorithm;
import com.dangdang.ddframe.rdb.sharding.config.yaml.hash.internel.HashConfig;
import com.dangdang.ddframe.rdb.sharding.id.generator.self.CommonSelfIdGenerator;
import com.dangdang.ddframe.rdb.sharding.jdbc.ShardingDataSource;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author DonneyYoung
 **/
public class HashShardingDataSource extends ShardingDataSource {

    private static final String DATABASE_KEY = "database";

    private static final String TABLES_KEY = "tables";

    private static final String DATABASE_ALGORITHM = "com.dangdang.ddframe.rdb.sharding.config.yaml.hash.algorithm.HashDatabaseAlgorithm";

    private static final String TABLES_ALGORITHM = "com.dangdang.ddframe.rdb.sharding.config.yaml.hash.algorithm.HashAlgorithm";

    public HashShardingDataSource(final File yamlFile) throws IOException {
        super(new ShardingRuleBuilder(yamlFile.getName(), unmarshal(yamlFile)).build(), unmarshal(yamlFile).getProps());
    }

    private static HashConfig unmarshal(final File yamlFile) throws IOException {
        try (
                FileInputStream fileInputStream = new FileInputStream(yamlFile);
                InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8")
        ) {
            HashConfig hashConfig = new Yaml(new Constructor(HashConfig.class)).loadAs(inputStreamReader, HashConfig.class);
            if (null != hashConfig.getHash())
                initHash(hashConfig);
            return hashConfig;
        }
    }

    private static void initHash(final HashConfig hashConfig) {
        Map<String, TableRuleConfig> tablesMap = hashConfig.getTables();
        Map<String, TreeMap<Long, TreeMap<Long, TreeMap<Long, String>>>> logicTableMap = new HashMap<>();
        for (Map.Entry<String, Map<Date, Map<Integer, Map<Integer, Object>>>> logicTableEntry : hashConfig.getHash().entrySet()) {
            TreeMap<Long, TreeMap<Long, TreeMap<Long, String>>> timestampMap = new TreeMap<>();
            for (Map.Entry<Date, Map<Integer, Map<Integer, Object>>> timeEntry : logicTableEntry.getValue().entrySet()) {
                TreeMap<Long, TreeMap<Long, String>> dsHashMap = new TreeMap<>();
                for (Map.Entry<Integer, Map<Integer, Object>> databaseEntry : timeEntry.getValue().entrySet()) {
                    TreeMap<Long, String> tbHashMap = new TreeMap<>();
                    @SuppressWarnings("SuspiciousMethodCalls")
                    String dsName = (String) databaseEntry.getValue().get(DATABASE_KEY);
                    @SuppressWarnings("unchecked,SuspiciousMethodCalls")
                    Map<Integer, String> tables = (Map<Integer, String>) databaseEntry.getValue().get(TABLES_KEY);
                    for (Map.Entry<Integer, String> tableEntry : tables.entrySet()) {
                        tbHashMap.put(Long.valueOf(tableEntry.getKey()), dsName + "." + tableEntry.getValue());
                    }
                    dsHashMap.put(Long.valueOf(databaseEntry.getKey()), tbHashMap);
                }
                timestampMap.put(timeEntry.getKey().getTime() - CommonSelfIdGenerator.SJDBC_EPOCH, dsHashMap);
            }
            String[] shardingSplit = logicTableEntry.getKey().split("\\.");
            String logicTable = shardingSplit[0];
            String shardingColumn = shardingSplit[1];
            logicTableMap.put(logicTable, timestampMap);
            StrategyConfig databaseStrategy = new StrategyConfig();
            databaseStrategy.setShardingColumns(shardingColumn);
            databaseStrategy.setAlgorithmClassName(DATABASE_ALGORITHM);
            StrategyConfig tableStrategy = new StrategyConfig();
            tableStrategy.setShardingColumns(shardingColumn);
            tableStrategy.setAlgorithmClassName(TABLES_ALGORITHM);
            TableRuleConfig tableRuleConfig = new TableRuleConfig();
            tableRuleConfig.setDynamic(true);
            tableRuleConfig.setDatabaseStrategy(databaseStrategy);
            tableRuleConfig.setTableStrategy(tableStrategy);
            tablesMap.put(logicTable, tableRuleConfig);
        }
        HashAlgorithm.getHASH().putAll(logicTableMap);
    }
}

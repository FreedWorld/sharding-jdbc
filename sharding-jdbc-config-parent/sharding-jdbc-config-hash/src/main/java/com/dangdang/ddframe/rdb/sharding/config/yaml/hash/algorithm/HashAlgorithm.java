package com.dangdang.ddframe.rdb.sharding.config.yaml.hash.algorithm;

import com.dangdang.ddframe.rdb.sharding.api.ShardingValue;
import com.dangdang.ddframe.rdb.sharding.api.strategy.table.SingleKeyTableShardingAlgorithm;
import com.google.common.collect.Range;
import lombok.Getter;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author DonneyYoung
 **/
public final class HashAlgorithm implements SingleKeyTableShardingAlgorithm<Long> {

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    @Getter
    private static final Map<String, TreeMap<Long, TreeMap<Long, TreeMap<Long, String>>>> HASH = new HashMap<>();

    private static final long TIMESTAMP_SHIFT = 22L;

    private static final long SECOND_RIGHT_SHIFT = 32L;

    private static final long SECOND_LEFT_SHIFT = 10L;

    private static final long DATABASE_SHIFT = 16L;

    private static final long DATABASE_MAX = 0xFFFF;

    private static final long TABLE_MAX = 0xFFFF;


    private String getSharding(String logicTableName, Long shardingValue) {
        int hashValue = hashLong(shardingValue);
        return HASH.get(logicTableName)
                .floorEntry(shardingValue >>> TIMESTAMP_SHIFT).getValue()
                .floorEntry((long) (hashValue >>> DATABASE_SHIFT)).getValue()
                .floorEntry(hashValue & TABLE_MAX).getValue();
    }

    @Override
    public String doEqualSharding(final Collection<String> availableTargetNames, final ShardingValue<Long> shardingValue) {
        return this.getSharding(shardingValue.getLogicTableName(), shardingValue.getValue());
    }

    @Override
    public Collection<String> doInSharding(final Collection<String> availableTargetNames, final ShardingValue<Long> shardingValue) {
        Collection<String> result = new LinkedHashSet<>();
        for (Long each : shardingValue.getValues()) {
            result.add(this.getSharding(shardingValue.getLogicTableName(), each));
        }
        return result;
    }

    @Override
    public Collection<String> doBetweenSharding(final Collection<String> availableTargetNames, final ShardingValue<Long> shardingValue) {
        Collection<String> result = new LinkedHashSet<>();
        Range<Long> range = shardingValue.getValueRange();
        TreeMap<Long, TreeMap<Long, TreeMap<Long, String>>> timestampMap = HASH.get(shardingValue.getLogicTableName());
        long lowerTime = (range.lowerEndpoint() >>> SECOND_RIGHT_SHIFT + 1) << SECOND_LEFT_SHIFT;
        long upperTime = (range.upperEndpoint() >>> SECOND_RIGHT_SHIFT - 1) << SECOND_LEFT_SHIFT;
        long lowerDatabaseHash = range.lowerEndpoint() >>> DATABASE_SHIFT & DATABASE_MAX;
        long upperDatabaseHash = range.upperEndpoint() >>> DATABASE_SHIFT & DATABASE_MAX;
        long lowerTableHash = range.lowerEndpoint() & DATABASE_MAX;
        long upperTableHash = range.upperEndpoint() & DATABASE_MAX;
        for (TreeMap<Long, TreeMap<Long, String>> eachDatabaseMap : timestampMap.subMap(timestampMap.floorKey(lowerTime), upperTime + 1).values()) {
            for (TreeMap<Long, String> eachTableMap : eachDatabaseMap.subMap(eachDatabaseMap.floorKey(lowerDatabaseHash), upperDatabaseHash + 1L).values()) {
                for (String eachTable : eachTableMap.subMap(eachTableMap.floorKey(lowerTableHash), upperTableHash + 1L).values()) {
                    result.add(eachTable);
                }
            }
        }
        return result;
    }

    /**
     * MurmurHash2算法Java实现
     *
     * @param data 需要计算哈希的long值
     * @return 哈希值
     */
    public static int hashLong(long data) {
        int m = 0x5bd1e995;
        int r = 24;
        int h = 0;
        int k = (int) data * m;
        k ^= k >>> r;
        h ^= k * m;
        k = (int) (data >> 32) * m;
        k ^= k >>> r;
        h *= m;
        h ^= k * m;
        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }

}

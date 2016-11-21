package com.dangdang.ddframe.rdb.sharding.config.yaml.hash.algorithm;

import com.dangdang.ddframe.rdb.sharding.api.ShardingValue;
import com.dangdang.ddframe.rdb.sharding.api.strategy.database.SingleKeyDatabaseShardingAlgorithm;

import java.util.Collection;
import java.util.LinkedHashSet;

/**
 * @author DonneyYoung
 **/
public final class HashDatabaseAlgorithm implements SingleKeyDatabaseShardingAlgorithm<Long> {

    private static String mark;

    private static Collection<String> result;

    private String getSharding(String logicTableName) {
        if (null == mark) {
            mark = HashAlgorithm.getHASH()
                    .get(logicTableName)
                    .floorEntry(0L).getValue()
                    .floorEntry(0L).getValue()
                    .floorEntry(0L).getValue()
                    .split("\\.")[0];
            result = new LinkedHashSet<>();
            result.add(mark);
        }
        return mark;
    }

    @Override
    public String doEqualSharding(final Collection<String> availableTargetNames, final ShardingValue<Long> shardingValue) {
        if (null == mark)
            getSharding(shardingValue.getLogicTableName());
        return mark;
    }

    @Override
    public Collection<String> doInSharding(final Collection<String> availableTargetNames, final ShardingValue<Long> shardingValue) {
        if (null == result)
            getSharding(shardingValue.getLogicTableName());
        return result;
    }

    @Override
    public Collection<String> doBetweenSharding(final Collection<String> availableTargetNames, final ShardingValue<Long> shardingValue) {
        if (null == result)
            getSharding(shardingValue.getLogicTableName());
        return result;
    }

}

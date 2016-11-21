package com.dangdang.ddframe.rdb.sharding.config.yaml.hash.entity;

import com.dangdang.ddframe.rdb.sharding.config.yaml.hash.algorithm.HashAlgorithm;
import lombok.Data;

/**
 * @author DonneyYoung
 **/
@Data
public class SnowflakeHash {

    private Long id;

    private Long timeKey;

    private Long databaseKey;

    private Long tableKey;

    public SnowflakeHash() {
    }

    public SnowflakeHash(long id) {
        int hash = HashAlgorithm.hashLong(id);
        this.id = id;
        this.timeKey = id >>> 22L;
        this.databaseKey = hash >>> 16 & 65535L;
        this.tableKey = hash & 65535L;
    }
}

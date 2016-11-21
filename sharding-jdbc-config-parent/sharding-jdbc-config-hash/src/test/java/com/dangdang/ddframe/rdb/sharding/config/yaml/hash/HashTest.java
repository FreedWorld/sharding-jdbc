package com.dangdang.ddframe.rdb.sharding.config.yaml.hash;

import com.dangdang.ddframe.rdb.sharding.config.yaml.hash.entity.SnowflakeHash;
import com.dangdang.ddframe.rdb.sharding.config.yaml.hash.mapper.SqlMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author DonneyYoung
 **/
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"/spring/applicationContext-database.xml"})
public class HashTest {

    @Autowired
    @Qualifier("sqlMapper")
    private SqlMapper sqlMapper;

    @Autowired
    @Qualifier("db0Mapper")
    private SqlMapper db0Mapper;

    @Autowired
    @Qualifier("db1Mapper")
    private SqlMapper db1Mapper;

    @Autowired
    @Qualifier("db2Mapper")
    private SqlMapper db2Mapper;

    @Autowired
    @Qualifier("db3Mapper")
    private SqlMapper db3Mapper;

    @Test
    public void testWithDataSource() {
        db0Mapper.delete("DELETE FROM t_snowflake_hash_0");
        db0Mapper.delete("DELETE FROM t_snowflake_hash_1");
        db1Mapper.delete("DELETE FROM t_snowflake_hash_2");
        db1Mapper.delete("DELETE FROM t_snowflake_hash_3");
        db2Mapper.delete("DELETE FROM t_snowflake_hash_4");
        db2Mapper.delete("DELETE FROM t_snowflake_hash_5");
        db3Mapper.delete("DELETE FROM t_snowflake_hash_6");
        db3Mapper.delete("DELETE FROM t_snowflake_hash_7");

        List<Long> idList = new ArrayList<>();
        idList.add(362387865600007L);
        idList.add(362387865600001L);
        idList.add(362387865600005L);
        idList.add(362387865600000L);
        idList.add(1206117853230864393L);
        idList.add(1206117853230864385L);
        idList.add(1206117853230864389L);
        idList.add(1206117853230864384L);
        for (Long id : idList) {
            SnowflakeHash iSnowflakeHash = new SnowflakeHash(id);
            sqlMapper.insert("INSERT INTO snowflake_hash(id,time_key,database_key,table_key) VALUES(#{id},#{timeKey},#{databaseKey},#{tableKey})", iSnowflakeHash);
        }

        SnowflakeHash db0tb0 = db0Mapper.selectOne("SELECT * FROM t_snowflake_hash_0 WHERE id = #{id}", 362387865600007L, SnowflakeHash.class);
        assertThat(db0tb0,equalTo(new SnowflakeHash(362387865600007L)));
        SnowflakeHash db0tb1 = db0Mapper.selectOne("SELECT * FROM t_snowflake_hash_1 WHERE id = #{id}", 362387865600001L, SnowflakeHash.class);
        assertThat(db0tb1,equalTo(new SnowflakeHash(362387865600001L)));
        SnowflakeHash db1tb2 = db1Mapper.selectOne("SELECT * FROM t_snowflake_hash_2 WHERE id = #{id}", 362387865600005L, SnowflakeHash.class);
        assertThat(db1tb2,equalTo(new SnowflakeHash(362387865600005L)));
        SnowflakeHash db1tb3 = db1Mapper.selectOne("SELECT * FROM t_snowflake_hash_3 WHERE id = #{id}", 362387865600000L, SnowflakeHash.class);
        assertThat(db1tb3,equalTo(new SnowflakeHash(362387865600000L)));
        SnowflakeHash db2tb4 = db2Mapper.selectOne("SELECT * FROM t_snowflake_hash_4 WHERE id = #{id}", 1206117853230864393L, SnowflakeHash.class);
        assertThat(db2tb4,equalTo(new SnowflakeHash(1206117853230864393L)));
        SnowflakeHash db2tb5 = db2Mapper.selectOne("SELECT * FROM t_snowflake_hash_5 WHERE id = #{id}", 1206117853230864385L, SnowflakeHash.class);
        assertThat(db2tb5,equalTo(new SnowflakeHash(1206117853230864385L)));
        SnowflakeHash db3tb6 = db3Mapper.selectOne("SELECT * FROM t_snowflake_hash_6 WHERE id = #{id}", 1206117853230864389L, SnowflakeHash.class);
        assertThat(db3tb6,equalTo(new SnowflakeHash(1206117853230864389L)));
        SnowflakeHash db3tb7 = db3Mapper.selectOne("SELECT * FROM t_snowflake_hash_7 WHERE id = #{id}", 1206117853230864384L, SnowflakeHash.class);
        assertThat(db3tb7,equalTo(new SnowflakeHash(1206117853230864384L)));

        Map<String, Long> inShardingQueryMap = new HashMap<>();
        inShardingQueryMap.put("id1", 362387865600007L);
        inShardingQueryMap.put("id2", 1206117853230864393L);
        List<SnowflakeHash> inShardingResult = sqlMapper.selectList("SELECT * FROM snowflake_hash WHERE id IN (#{id1},#{id2})", inShardingQueryMap, SnowflakeHash.class);
        assertThat(inShardingResult.size(),is(inShardingQueryMap.size()));

        Map<String, Long> betweenShardingQueryMap = new HashMap<>();
        betweenShardingQueryMap.put("lower", 0L);
        betweenShardingQueryMap.put("upper", Long.MAX_VALUE);
        List<SnowflakeHash> betweenShardingResult = sqlMapper.selectList("SELECT * FROM snowflake_hash WHERE id BETWEEN #{lower} AND #{upper}", betweenShardingQueryMap, SnowflakeHash.class);
        assertThat(betweenShardingResult.size(),is(idList.size()));
    }
}

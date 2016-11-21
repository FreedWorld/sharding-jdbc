package com.dangdang.ddframe.rdb.sharding.config.yaml.hash.internel;

import com.dangdang.ddframe.rdb.sharding.config.yaml.internel.YamlConfig;
import lombok.Setter;
import lombok.Getter;

import java.util.Date;
import java.util.Map;

/**
 * @author DonneyYoung
 **/
@Getter
@Setter
public class HashConfig extends YamlConfig {

    private Map<String, Map<Date, Map<Integer, Map<Integer, Object>>>> hash;
}

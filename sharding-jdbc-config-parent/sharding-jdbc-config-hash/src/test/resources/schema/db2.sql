-- DROP SCHEMA IF EXISTS `db2`;
-- CREATE SCHEMA `db2`;
DROP TABLE IF EXISTS `t_snowflake_hash_4`;
DROP TABLE IF EXISTS `t_snowflake_hash_5`;
CREATE TABLE `t_snowflake_hash_4` (
  `id` bigint(20) NOT NULL,
  `time_key` bigint(20) NOT NULL,
  `database_key` bigint(20) NOT NULL,
  `table_key` bigint(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `t_snowflake_hash_5` (
  `id` bigint(20) NOT NULL,
  `time_key` bigint(20) NOT NULL,
  `database_key` bigint(20) NOT NULL,
  `table_key` bigint(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

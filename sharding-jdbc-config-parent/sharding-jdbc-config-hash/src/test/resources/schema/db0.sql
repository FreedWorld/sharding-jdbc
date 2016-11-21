-- DROP SCHEMA IF EXISTS `db0`;
-- CREATE SCHEMA `db0`;
DROP TABLE IF EXISTS `t_snowflake_hash_0`;
DROP TABLE IF EXISTS `t_snowflake_hash_1`;
CREATE TABLE `t_snowflake_hash_0` (
  `id` bigint(20) NOT NULL,
  `time_key` bigint(20) NOT NULL,
  `database_key` bigint(20) NOT NULL,
  `table_key` bigint(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `t_snowflake_hash_1` (
  `id` bigint(20) NOT NULL,
  `time_key` bigint(20) NOT NULL,
  `database_key` bigint(20) NOT NULL,
  `table_key` bigint(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

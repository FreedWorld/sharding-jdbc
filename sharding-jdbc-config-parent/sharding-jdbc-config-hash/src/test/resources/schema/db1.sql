-- DROP SCHEMA IF EXISTS `db1`;
-- CREATE SCHEMA `db1`;
DROP TABLE IF EXISTS `t_snowflake_hash_2`;
DROP TABLE IF EXISTS `t_snowflake_hash_3`;
CREATE TABLE `t_snowflake_hash_2` (
  `id` bigint(20) NOT NULL,
  `time_key` bigint(20) NOT NULL,
  `database_key` bigint(20) NOT NULL,
  `table_key` bigint(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `t_snowflake_hash_3` (
  `id` bigint(20) NOT NULL,
  `time_key` bigint(20) NOT NULL,
  `database_key` bigint(20) NOT NULL,
  `table_key` bigint(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

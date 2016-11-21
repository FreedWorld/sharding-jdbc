-- DROP SCHEMA IF EXISTS `db3`;
-- CREATE SCHEMA `db3`;
DROP TABLE IF EXISTS `t_snowflake_hash_6`;
DROP TABLE IF EXISTS `t_snowflake_hash_7`;
CREATE TABLE `t_snowflake_hash_6` (
  `id` bigint(20) NOT NULL,
  `time_key` bigint(20) NOT NULL,
  `database_key` bigint(20) NOT NULL,
  `table_key` bigint(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `t_snowflake_hash_7` (
  `id` bigint(20) NOT NULL,
  `time_key` bigint(20) NOT NULL,
  `database_key` bigint(20) NOT NULL,
  `table_key` bigint(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

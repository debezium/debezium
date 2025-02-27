CREATE TABLE t_user_black_list (
    `id` int(10) unsigned NOT NULL,
    `data` varchar(20),
    `create_time` datetime,
    `update_time` datetime,
    PRIMARY KEY (`id`)
);

ALTER TABLE t_user_black_list
    MODIFY COLUMN `update_time` datetime(0) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(0) COMMENT 'update_time' AFTER create_time;

INSERT INTO t_user_black_list (`id`,`create_time`,`data`) VALUES (1, CURRENT_TIMESTAMP(), 'test');

UPDATE t_user_black_list SET `data` = 'test2' WHERE `id` = 1;
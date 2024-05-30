CREATE TABLE `roles` (
`id` varchar(32) NOT NULL,
`name` varchar(100) NOT NULL,
`context` varchar(20) NOT NULL,
`organization_id` int(11) DEFAULT NULL,
`client_id` varchar(32) NOT NULL,
`scope_action_ids` text NOT NULL,
PRIMARY KEY (`id`),
FULLTEXT KEY `scope_action_ids_idx` (`scope_action_ids`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

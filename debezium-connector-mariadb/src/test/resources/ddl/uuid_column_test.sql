CREATE TABLE `test_uuid` (
    `id` uuid NOT NULL,
    `expected` char(36),
    PRIMARY KEY (`id`)
);

INSERT INTO test_uuid (`id`, `expected`) values ( '01020304-0506-0708-090a-0b0c0d0e0f10', '01020304-0506-0708-090a-0b0c0d0e0f10');
INSERT INTO test_uuid (`id`, `expected`) values ( '00000000-0000-0000-0000-000000000000', '00000000-0000-0000-0000-000000000000');
INSERT INTO test_uuid (`id`, `expected`) values ( '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000001');
INSERT INTO test_uuid (`id`, `expected`) values ( 'ffffffff-ffff-ffff-ffff-ffffffffffff', 'ffffffff-ffff-ffff-ffff-ffffffffffff');

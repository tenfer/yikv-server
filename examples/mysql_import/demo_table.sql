-- 本地演示：与 import.sql / ../../schema.json 字段对齐
CREATE DATABASE IF NOT EXISTS yikv_demo;
USE yikv_demo;

DROP TABLE IF EXISTS dsp_rows;
CREATE TABLE dsp_rows (
    `key`           VARCHAR(256) NOT NULL,
    duf_inner_pkg   VARCHAR(512) NULL,
    duf_inner_all   VARCHAR(512) NULL,
    duf_outer_pkg   VARCHAR(512) NULL,
    duf_all_all     VARCHAR(512) NULL,
    PRIMARY KEY (`key`)
);

INSERT INTO dsp_rows (`key`, duf_inner_pkg, duf_inner_all, duf_outer_pkg, duf_all_all) VALUES
('demo-1', 'a', 'b', 'c', 'd'),
('demo-2', 'x', 'y', 'z', 'w');

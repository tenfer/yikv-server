-- 列名/别名须与 yikv schema.json 中 fields[].name 一致（示例见 ../../schema.json）
SELECT
    `key`,
    duf_inner_pkg,
    duf_inner_all,
    duf_outer_pkg,
    duf_all_all
FROM dsp_rows
WHERE 1 = 1;

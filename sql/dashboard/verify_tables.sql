SELECT table_schema, table_name, comment
FROM workspace.information_schema.tables
WHERE table_schema IN ('bronze', 'silver', 'gold')
ORDER BY table_schema, table_name;

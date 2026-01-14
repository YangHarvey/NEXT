```bash
./secondary_index_write_tpch [db_path] [data_size] [dataset_path]
# example
./secondary_index_write_tpch "/test/next_db" 6000000 "/home/ysm/tpch-data/lineitem.1"
./secondary_index_write_tpch "/home/ysm/work/test/next_db" 600000000 "/home/ysm/work/tpch-data/lineitem.100"

./secondary_index_write_tpch "/home/ysm/work/test/next_db_1" 6000000 "/home/ysm/work/tpch-data/lineitem.1"

./secondary_index_write_tpch "/home/ysm/work/test/next_db_10" 60000000 "/home/ysm/work/tpch-data/lineitem.10"

./secondary_index_query [db_path] [query_size] [query_path]

./secondary_index_query "/home/ysm/work/test/next_db" 5 "/home/ysm/work/NEXT/tpch_query/query_q6"
./secondary_index_query "/home/ysm/work/test/next_db_1" 5 "/home/ysm/work/NEXT/tpch_query/query_q6"
./secondary_index_query "/home/ysm/work/test/next_db_10" 5 "/home/ysm/work/NEXT/tpch_query/query_q6"
```
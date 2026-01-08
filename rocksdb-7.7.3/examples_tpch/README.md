```bash
./secondary_index_write_tpch [db_path] [data_size] [dataset_path]
# example
./secondary_index_write_tpch "/test/next_db" 6000000 "/home/ysm/tpch-data/lineitem.1"

./secondary_index_query [db_path] [query_size] [query_path]

./secondary_index_query "/test/next_db" 5 "/home/ysm/NEXT/tpch_query/query_q6"
```
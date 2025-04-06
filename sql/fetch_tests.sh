#!/bin/bash

n=10

declare -A tests
tests["managed_external_test_managed"]="/home/sql/managed_external_test/managed.sql"
tests["managed_external_test_external"]="/home/sql/managed_external_test/external.sql"
tests["parquet_textfile_test_parquet"]="/home/sql/parquet_textfile_test/parquet.sql"
tests["parquet_textfile_test_textfile"]="/home/sql/parquet_textfile_test/textfile.sql"
tests["partitioned_nonpartitioned_test_partitioned"]="/home/sql/partitioned_nonpartitioned_test/partitioned.sql"
tests["partitioned_nonpartitioned_test_nonpartitioned"]="/home/sql/partitioned_nonpartitioned_test/nonpartitioned.sql"

final_summary="Результаты сравнительного теста производительности:"
for test in "${!tests[@]}"; do
  sql_file="${tests[$test]}"
  total_time=0
  echo "Запускаем тест '$test' используя SQL файл: $sql_file"
  for (( i=1; i<=n; i++ )); do
    current_log="${test}_${i}.log"
    hive -f "$sql_file" > "$current_log" 2>&1
    run_time=$(grep "Time taken:" "$current_log" | awk '{sum+=$3} END {print sum}')
    total_time=$(awk -v a="$total_time" -v b="$run_time" 'BEGIN {print a+b}')
    echo "    Тест '$test', запуск $i: $run_time seconds"
  done
  avg_time=$(awk -v total="$total_time" -v count="$n" 'BEGIN {print total/count}')
  final_summary+="\nСреднее время '$test' по $n запускам: $avg_time seconds"
done

echo -e "$final_summary"

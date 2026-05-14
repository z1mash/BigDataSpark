MASTER="lab2_spark_master"
PG_JAR="/spark-jobs/jars/postgresql-42.7.3.jar"
CH_JAR="/spark-jobs/jars/clickhouse-jdbc-0.6.0-all.jar"

run_star() {
  echo "=== Running ETL: mock_data -> Star Schema (PostgreSQL) ==="
  docker exec $MASTER /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars $PG_JAR \
    /spark-jobs/etl_to_star.py
}

run_clickhouse() {
  echo "=== Running ETL: Star Schema -> Reports (ClickHouse) ==="
  docker exec $MASTER /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars $PG_JAR,$CH_JAR \
    /spark-jobs/etl_to_clickhouse.py
}

case "$1" in
  star)        run_star ;;
  clickhouse)  run_clickhouse ;;
  all)         run_star && run_clickhouse ;;
  *)
    echo "Usage: bash run_jobs.sh [star|clickhouse|all]"
    exit 1
    ;;
esac

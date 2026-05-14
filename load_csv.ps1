$container = "lab2_postgres"
$db = "lab2"
$user = "user"
$table = "mock_data"

$files = @(
    "/csv_data/MOCK_DATA.csv",
    "/csv_data/MOCK_DATA (1).csv",
    "/csv_data/MOCK_DATA (2).csv",
    "/csv_data/MOCK_DATA (3).csv",
    "/csv_data/MOCK_DATA (4).csv",
    "/csv_data/MOCK_DATA (5).csv",
    "/csv_data/MOCK_DATA (6).csv",
    "/csv_data/MOCK_DATA (7).csv",
    "/csv_data/MOCK_DATA (8).csv",
    "/csv_data/MOCK_DATA (9).csv"
)

foreach ($file in $files) {
    Write-Host "Loading $file ..."
    docker exec $container psql -U $user -d $db -c "\COPY $table FROM '$file' CSV HEADER;"
}

Write-Host "Done! Checking row count..."
docker exec $container psql -U $user -d $db -c "SELECT COUNT(*) FROM $table;"

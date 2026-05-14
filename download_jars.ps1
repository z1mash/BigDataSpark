$jarsDir = "$PSScriptRoot\spark-jobs\jars"

Write-Host "Downloading postgresql JDBC driver..."
Invoke-WebRequest -Uri "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar" `
    -OutFile "$jarsDir\postgresql-42.7.3.jar"

Write-Host "Downloading ClickHouse JDBC driver..."
Invoke-WebRequest -Uri "https://github.com/ClickHouse/clickhouse-java/releases/download/v0.6.0/clickhouse-jdbc-0.6.0-all.jar" `
    -OutFile "$jarsDir\clickhouse-jdbc-0.6.0-all.jar"

Write-Host "Done!"
Get-ChildItem $jarsDir

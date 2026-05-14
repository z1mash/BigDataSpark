CONTAINER="lab2_postgres"
DB="lab2"
USER="user"
TABLE="mock_data"

FILES=(
  "/csv_data/MOCK_DATA.csv"
  "/csv_data/MOCK_DATA (1).csv"
  "/csv_data/MOCK_DATA (2).csv"
  "/csv_data/MOCK_DATA (3).csv"
  "/csv_data/MOCK_DATA (4).csv"
  "/csv_data/MOCK_DATA (5).csv"
  "/csv_data/MOCK_DATA (6).csv"
  "/csv_data/MOCK_DATA (7).csv"
  "/csv_data/MOCK_DATA (8).csv"
  "/csv_data/MOCK_DATA (9).csv"
)

for FILE in "${FILES[@]}"; do
  echo "Loading $FILE ..."
  docker exec -i $CONTAINER psql -U $USER -d $DB -c "\COPY $TABLE FROM '$FILE' CSV HEADER;"
done

echo "Done! Checking row count..."
docker exec -i $CONTAINER psql -U $USER -d $DB -c "SELECT COUNT(*) FROM $TABLE;"

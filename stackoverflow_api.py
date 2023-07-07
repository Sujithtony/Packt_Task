import requests
import psycopg2

# API authentication
api_key = "YOUR_API_KEY"

# Database connection details
db_host = "YOUR_DB_HOST"
db_name = "YOUR_DB_NAME"
db_user = "YOUR_DB_USER"
db_password = "YOUR_DB_PASSWORD"

# API endpoint and query parameters
api_url = "https://api.stackexchange.com/2.3/questions"
params = {
    "site": "stackoverflow",
    "order": "desc",
    "sort": "creation",
    "fromdate": "1672531200",  # Example: Unix timestamp for a specific month
    "todate": "1675209599"  # Example: Unix timestamp for the end of the month
}

# Fetch data from the Stack Overflow API
response = requests.get(api_url, params=params)
data = response.json()["items"]

# Establish database connection
conn = psycopg2.connect(
    host=db_host,
    database=db_name,
    user=db_user,
    password=db_password
)

# Create a database cursor
cursor = conn.cursor()

# Create a table to store the fetched data
create_table_query = """
CREATE TABLE stack_overflow_data (
    question_id INTEGER PRIMARY KEY,
    title TEXT,
    tags TEXT[],
    creation_date TIMESTAMP
)
"""
cursor.execute(create_table_query)

# Insert the fetched data into the database
insert_query = """
INSERT INTO stack_overflow_data (question_id, title, tags, creation_date)
VALUES (%s, %s, %s, to_timestamp(%s))
"""
for item in data:
    question_id = item["question_id"]
    title = item["title"]
    tags = item["tags"]
    creation_date = item["creation_date"]
    cursor.execute(insert_query, (question_id, title, tags, creation_date))

# Commit the changes to the database
conn.commit()

# Execute the sample business query - Top trending tags
business_query = """
SELECT unnest(tags) as tag, COUNT(*) as count
FROM stack_overflow_data
GROUP BY tag
ORDER BY count DESC
LIMIT 10
"""
cursor.execute(business_query)
result = cursor.fetchall()

# Print the results
print("Top Trending Tags:")
for row in result:
    tag, count = row
    print(f"{tag}: {count}")

# Close the cursor and database connection
cursor.close()
conn.close()

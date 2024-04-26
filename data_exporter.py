import sqlite3
import csv
import configparser
import json

# Config and db connection
config = configparser.ConfigParser()
config.read("config.ini")
conn = sqlite3.connect(config['DEFAULT']['DatabasePath'])
cursor = conn.cursor()

# SQL query
query = """
    SELECT
    c.place_id as place,
    c.name as name,
    c.state as provincia,
    c.city as ciudad,
    c.postal_code as cp,
    c.address as address,
    IFNULL(cd.website, "https://rankingresidencias.com/no-web") as web,
    c.address || ', ' || c.state || ', ' || c.city || ', ' || c.postal_code as "dirección completa",
    cd.phone_number as teléfono,
    cd.total_reviews as reviews,
    cd.avg_reviews as media,
    cd.place_photo as foto,
    cd.updated_at,
    cd.opening_hours as horario,
    cd.reviews as destacadas,
    'https://www.google.com/search?q=' || REPLACE(REPLACE(name || ' ' || c.address, ' ', '+'), ',', '%2C') || '+opiniones' as "enlace a ficha google"
FROM
    company c
INNER JOIN
    company_details cd ON cd.place_id = c.place_id;
"""

cursor.execute(query)

# Get data query
rows = cursor.fetchall()


def unescape_text(text):
    try:
        parsed_json = json.loads(text)
        unescaped_json = json.dumps(parsed_json, ensure_ascii=False)
        return unescaped_json
    except json.JSONDecodeError:
        return text


# Unscape json strings
unescaped_rows = []
for row in rows:
    unescaped_row = list(row)
    unescaped_row[13] = unescape_text(unescaped_row[13])  # Horario
    unescaped_row[14] = unescape_text(unescaped_row[14])  # Destacadas
    unescaped_rows.append(unescaped_row)

# Export results into unescaped CSV UTF-8
with open('exported_data.csv', 'w', encoding='utf-8', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow([i[0] for i in cursor.description])  # write headers
    csvwriter.writerows(unescaped_rows)

# Close connection
conn.close()

import mysql.connector

mydb = mysql.connector.connect(
  host="35.222.7.78",
  user="digitalskola",
  password="D6GhCbaaiq8LlNy7",
  database="digitalskola"
)
mycursor = mydb.cursor()

mycursor.execute("DROP TABLE customers")



import mysql.connector
from mysql.connector import Error
from datetime import datetime
from config import DB_CONFIG

def save_to_mysql(timestamp, price1, result):
    try:
        connection=mysql.connector.connect(**DB_CONFIG)

        if connection.is_connected():
            cursor = connection.cursor()

            # Define your MySQL table schema here
            table_creation_query = """
            CREATE TABLE IF NOT EXISTS stock_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp DATETIME,
                stock_price INT,
                change_percentage INT
            );
            """
            cursor.execute(table_creation_query)

            # Insert data into the table
            insert_query = "INSERT INTO stock_data (timestamp, stock_price, change_percentage) VALUES (%s, %s, %s)"
            data = (timestamp, price1, result)
            cursor.execute(insert_query, data)

            connection.commit()
            print("Data saved to MySQL successfully.")

    except Error as e:
        print(f"Error: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    # Example usage
    current_timestamp = datetime.now().isoformat()
    save_to_mysql(current_timestamp, 100, 5)

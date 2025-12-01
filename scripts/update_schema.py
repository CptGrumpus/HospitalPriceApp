import sys
import os
from sqlalchemy import create_engine, text

# Define the database file
DB_URL = "sqlite:///hospital.db"
engine = create_engine(DB_URL, echo=False)

def add_column_if_not_exists(connection, table, column, col_type):
    try:
        # Check if column exists
        query = text(f"SELECT {column} FROM {table} LIMIT 1")
        connection.execute(query)
        print(f"Column '{column}' already exists in '{table}'.")
    except Exception:
        # If it fails, the column likely doesn't exist, so add it
        print(f"Adding column '{column}' to '{table}'...")
        try:
            alter_query = text(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
            connection.execute(alter_query)
            print("Success.")
        except Exception as e:
            print(f"Failed to add column: {e}")

def update_schema():
    print("--- Updating Database Schema for AI Generated Content ---")
    with engine.connect() as conn:
        # We need to allow autocommit for ALTER TABLE in some sqlite versions/wrappers, 
        # but SQLAlchemy usually handles it.
        
        # Add generated_title
        add_column_if_not_exists(conn, "code_definitions", "generated_title", "VARCHAR")
        
        # Add generated_description
        add_column_if_not_exists(conn, "code_definitions", "generated_description", "TEXT")
        
        # Add source_text (so we know what we based the generation on)
        add_column_if_not_exists(conn, "code_definitions", "source_text", "TEXT")
        
        # Add category (if we want to store the inferred category)
        add_column_if_not_exists(conn, "code_definitions", "category", "VARCHAR")
        
        conn.commit()
    print("--- Schema Update Complete ---")

if __name__ == "__main__":
    update_schema()


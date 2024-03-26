import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection details from environment variables
username = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
database = os.getenv('DB_DATABASE')

# Establish database connection
try:
    engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}/{database}', echo=False)
    logger.info("Database engine created successfully.")
except Exception as e:
    logger.error(f"Error creating database engine: {e}")
    raise e

excel_file = 'tourism_data.xlsx'

def excel_col_index_to_letter(index):
    letter = ''
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        letter = chr(65 + remainder) + letter
    return letter

def create_tables(conn):
    try:
        conn.execute(text("DROP TABLE IF EXISTS article_data;"))
        conn.execute(text("DROP TABLE IF EXISTS column_names;"))
        
        conn.execute(text("""
        CREATE TABLE column_names (
            id INT AUTO_INCREMENT PRIMARY KEY,
            column_name TEXT NOT NULL,
            excel_column_position VARCHAR(5)
        );
        """))
        
        conn.execute(text("""
        CREATE TABLE article_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            article_id INT,
            column_id INT,
            value TEXT,
            FOREIGN KEY (column_id) REFERENCES column_names(id)
        );
        """))
        logger.info("Tables created successfully.")
    except SQLAlchemyError as e:
        logger.error(f"Error creating tables: {e}")
        raise e

def insert_column_names(df, conn):
    column_ids = {}
    for index, col in enumerate(df.columns):
        col_letter = excel_col_index_to_letter(index + 1)
        try:
            conn.execute(text("""
            INSERT INTO column_names (column_name, excel_column_position)
            VALUES (:column_name, :excel_column_position)
            """), {'column_name': col, 'excel_column_position': col_letter})
            result = conn.execute(text("SELECT id FROM column_names WHERE column_name = :column_name"), {'column_name': col})
            column_id = result.fetchone()[0]
            column_ids[col] = column_id
            logger.info(f"Column '{col}' inserted with ID {column_id}.")
        except SQLAlchemyError as e:
            logger.error(f"Error inserting column name '{col}': {e}")
            raise e
    return column_ids

def insert_article_data_optimized(df, column_ids, conn):
    logger.info("Inserting article data in bulk...")
    bulk_data = []
    for index, row in df.iterrows():
        article_id = index + 1
        for col in df.columns:
            value = row[col]
            if col in column_ids:
                bulk_data.append({
                    'article_id': article_id,
                    'column_id': column_ids[col],
                    'value': value if not pd.isnull(value) else None
                })
                
    try:
        chunk_size = 1000
        for i in range(0, len(bulk_data), chunk_size):
            chunk = bulk_data[i:i + chunk_size]
            conn.execute(text("""
                INSERT INTO article_data (article_id, column_id, value)
                VALUES (:article_id, :column_id, :value)
            """), chunk)
        logger.info("Bulk data insertion completed successfully.")
    except SQLAlchemyError as e:
        logger.error(f"Error during bulk data insertion: {e}")
        raise e

with engine.begin() as conn:
    create_tables(conn)
    xls = pd.ExcelFile(excel_file)
    for sheet_name in xls.sheet_names:
        logger.info(f"Processing sheet: {sheet_name}")
        try:
            df = pd.read_excel(xls, sheet_name=sheet_name)
            column_ids = insert_column_names(df, conn)
            insert_article_data_optimized(df, column_ids, conn)
            logger.info(f"Sheet {sheet_name} processed successfully.")
        except Exception as e:
            logger.error(f"Error processing sheet '{sheet_name}': {e}")
            raise e

logger.info("Script execution completed.")

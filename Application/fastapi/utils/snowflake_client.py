import snowflake.connector
import pandas as pd
from config import fastapi_config

class SnowflakeClient:
    def __init__(self):
        self.conn = snowflake.connector.connect(
            account=fastapi_config.SNOWFLAKE_ACCOUNT,
            user=fastapi_config.SNOWFLAKE_USER,
            password=fastapi_config.SNOWFLAKE_PASSWORD,
            warehouse=fastapi_config.SNOWFLAKE_WAREHOUSE,
            database=fastapi_config.SNOWFLAKE_DATABASE,
            schema=fastapi_config.SNOWFLAKE_SCHEMA
        )
        pass

    def fetch_document_info(self):
        # df = self.create_fallback_dataframe()
        # print("fetch_document_info:", df)
        query = """
        SELECT title, summary, pdf_link, image_link
        FROM publications
        """

        cursor = self.conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        print("fetch_document_info:", df)
        cursor.close()
        return df


    def create_fallback_dataframe(self):
        # Create a DataFrame with dummy records for testing
        data = {
            'document_name': ['Dummy Document 1', 'Dummy Document 2'],
            's3_pdf_link': ['https://example.com/dummy1.pdf', 'https://example.com/dummy2.pdf'],
            'image_links': ['https://example.com/image1.png', 'https://example.com/image2.png'],
            'document_cover_image_link': ['https://example.com/cover1.png', 'https://example.com/cover2.png'],
            'summary': ['This is a summary for document 1.', 'This is a summary for document 2.']
        }
        print("create_fallback_dataframe:", data)
        return pd.DataFrame(data)

    def close_connection(self):
        self.conn.close()
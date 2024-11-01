from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import time
import boto3
import requests
import pathlib
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import snowflake.connector
import urllib.parse

# Load environment variables
env_path = pathlib.Path('/opt/airflow/.env')
load_dotenv(dotenv_path=env_path)

# Set up S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

bucket_name = os.getenv('AWS_BUCKET_NAME')
base_url = os.getenv('WEB_BASE_URL')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False
}

def scrape_data(**kwargs):
    chrome_options = Options()
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.binary_location = '/usr/bin/chromium'

    # Use the system-installed ChromeDriver
    driver = webdriver.Chrome(options=chrome_options)

    publications_data = []

    for page in range(10):
        if page==0:
            current_page_url = f"{base_url}first={page * 10}&sort=%40officialz32xdate%20descending" 
        else:
            current_page_url = f"{base_url}first={page * 10}&sort=%40officialz32xdate%20descending"
        # Open the current page
        driver.get(current_page_url)
        time.sleep(3)  # Wait for content to load

        # Find and store publication details on the main page
        publications = driver.find_elements(By.CSS_SELECTOR, 'div.coveo-result-row')
        main_page_publications = []

        for publication in publications:
            try:
                title = publication.find_element(By.CSS_SELECTOR, 'h4.coveo-title a').text.strip()
                publication_url = publication.find_element(By.CSS_SELECTOR, 'h4.coveo-title a').get_attribute('href')
                try:
                    image_url = publication.find_element(By.CSS_SELECTOR, 'img.coveo-result-image').get_attribute('src')
                except:
                    image_url = "Image not found"
            except Exception as e:
                title = "Title not found"
                publication_url = None
                image_url = "Image not found"
            
            if publication_url and not any(d["Publication URL"] == publication_url for d in main_page_publications):
                main_page_publications.append({"Title": title, "Publication URL": publication_url, "Image URL": image_url})

        # Loop through the publications collected from the main page
        for pub in main_page_publications:
            driver.get(pub["Publication URL"])
            time.sleep(3)

            # Extract summary
            try:
                summary_elements = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.article__paragraph p'))
                )
                summary = ' '.join([para.text.strip() for para in summary_elements if para.text.strip()]).replace("\xa0", " ").strip()
            except:
            # If that doesn't exist, fall back to extracting from a more generic structure
                try:
                    summary_elements = WebDriverWait(driver, 10).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div p'))
                    )
                except:
                    summary_elements = []
            if summary_elements:
                summary = ' '.join([para.text.strip() for para in summary_elements if para.text.strip()])
                summary = summary.replace("\xa0", " ").strip()  # Handle non-breaking spaces
            else:
                summary = "No summary found"
            
            # Extract PDF link
            try:
                pdf_link_element = driver.find_element(By.CSS_SELECTOR, 'a.content-asset.content-asset--primary')
                pdf_link = pdf_link_element.get_attribute('href')
            except:
                pdf_link = "No PDF link found"

            # Download and upload PDF to S3
            pdf_s3_link = None
            if pdf_link != "No PDF link found":
                pdf_content = requests.get(pdf_link).content
                pdf_s3_path = f'staging/pdfs/{pub["Title"]}.pdf'
                pdf_s3_path_web = f'staging/pdfs/{urllib.parse.quote(pub["Title"])}.pdf'
                s3.put_object(Body=pdf_content, Bucket=bucket_name, Key=pdf_s3_path)
                pdf_s3_link = f'https://{bucket_name}.s3.amazonaws.com/{pdf_s3_path_web}'

            # Download and upload image to S3
            image_s3_link = None
            if pub["Image URL"] != "Image not found":
                image_content = requests.get(pub["Image URL"]).content
                image_s3_path = f'staging/images/{pub["Title"]}.jpg'
                image_s3_path_web = f'staging/images/{urllib.parse.quote(pub["Title"])}.jpg'
                s3.put_object(Body=image_content, Bucket=bucket_name, Key=image_s3_path)
                image_s3_link = f'https://{bucket_name}.s3.amazonaws.com/{image_s3_path_web}'

            # Append data for Snowflake insertion
            if not any(d.get("Publication URL") == pub["Publication URL"] for d in publications_data):
                publications_data.append({
                    "Title": pub["Title"],
                    "Summary": summary,
                    "PDF Link": pdf_s3_link,
                    "Image Link": image_s3_link
                })

    driver.quit()
    print("Data extraction and S3 upload complete.")
    
    # Push to XCom for the next Airflow task
    kwargs['ti'].xcom_push(key='publications_data', value=publications_data)

    return publications_data 


def upload_to_snowflake(**kwargs):
    publications_data = kwargs['ti'].xcom_pull(key='publications_data')

    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )
    cursor = conn.cursor()
    cursor.execute("""
        CREATE OR REPLACE TABLE PUBLICATIONS_TBL (
            title STRING,
            summary STRING,
            pdf_link STRING,
            image_link STRING
        )
    """)

    merge_query = """
                MERGE INTO PUBLICATIONS_TBL AS target
                USING (SELECT %s AS title, %s AS summary, %s AS pdf_link, %s AS image_link) AS source
                ON target.title = source.title
                WHEN MATCHED THEN
                    UPDATE SET summary = source.summary, pdf_link = source.pdf_link, image_link = source.image_link
                WHEN NOT MATCHED THEN
                    INSERT (title, summary, pdf_link, image_link)
                    VALUES (source.title, source.summary, source.pdf_link, source.image_link);
                """
    
    for pub in publications_data:
        cursor.execute(merge_query, (pub["Title"], pub["Summary"], pub["PDF Link"], pub["Image Link"]))

    insert_query = "INSERT INTO PUBLICATIONS_TBL (title, summary, pdf_link, image_link) VALUES (%s, %s, %s, %s)"
    for pub in publications_data:
        cursor.execute(insert_query, (pub["Title"], pub["Summary"], pub["PDF Link"], pub["Image Link"]))


    cursor.execute("""
            CREATE OR REPLACE TABLE PUBLICATIONS_TBL_CLEAN AS
            SELECT *
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY title ORDER BY 1) AS row_num
                FROM PUBLICATIONS_TBL
            )
            WHERE row_num = 1;

            ALTER TABLE PUBLICATIONS_TBL RENAME TO PUBLICATIONS_TBL_OLD;
            ALTER TABLE PUBLICATIONS_TBL_CLEAN RENAME TO PUBLICATIONS_TBL;

            DROP TABLE PUBLICATIONS_TBL_OLD;
        """)
    conn.commit()
    cursor.close()
    conn.close()
    print("Data inserted successfully!")


with DAG(
    'cfa_publications_pipeline',
    default_args=default_args,
    description='Automated data scraping, S3 upload, and Snowflake insertion',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    scrape_data_task = PythonOperator(
        task_id='scrape_data_task',
        python_callable=scrape_data
    )

    upload_to_snowflake_task = PythonOperator(
        task_id='upload_to_snowflake_task',
        python_callable=upload_to_snowflake
    )

    scrape_data_task >>  upload_to_snowflake_task

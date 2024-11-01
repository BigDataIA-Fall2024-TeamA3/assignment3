from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import time
import boto3
import requests
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Set up Chrome options for headless browsing
chrome_options = Options()
chrome_options.add_argument("--headless")

# Initialize WebDriver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# Initialize S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)
bucket_name = 'bigdata7245'  # Update with your S3 bucket name

# Base URL for publication pages
base_url = "https://rpc.cfainstitute.org/en/research-foundation/publications#"
publications_data = []  # To store publication data for Snowflake insertion

# Loop through each page (first 10 pages)
for page in range(10):
    current_page_url = f"{base_url}first={page * 10}&sort=%40officialz32xdate%20descending"
    driver.get(current_page_url)
    time.sleep(3)  # Wait for content to load

    # Find and store publication details on the main page
    publications = driver.find_elements(By.CSS_SELECTOR, 'div.coveo-result-row')
    main_page_publications = []

    for publication in publications:
        # Extract publication title, URL, and image URL
        try:
            title = publication.find_element(By.CSS_SELECTOR, 'h4.coveo-title a').text.strip()
            publication_url = publication.find_element(By.CSS_SELECTOR, 'h4.coveo-title a').get_attribute('href')
            image_url = publication.find_element(By.CSS_SELECTOR, 'img.coveo-result-image').get_attribute('src')
        except:
            continue  # Skip publication if any required element is missing

        if not any(d["Publication URL"] == publication_url for d in main_page_publications):
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
            try:
                pdf_content = requests.get(pdf_link).content
                pdf_s3_path = f'staging/pdfs/{pub["Title"].replace("/", "_")}.pdf'
                s3.put_object(Body=pdf_content, Bucket=bucket_name, Key=pdf_s3_path)
                pdf_s3_link = f's3://{bucket_name}/{pdf_s3_path}'
            except Exception as e:
                print(f"Error uploading PDF for {pub['Title']}: {e}")

        # Download and upload image to S3
        image_s3_link = None
        if pub["Image URL"] != "Image not found":
            try:
                image_content = requests.get(pub["Image URL"]).content
                image_s3_path = f'staging/images/{pub["Title"].replace("/", "_")}.jpg'
                s3.put_object(Body=image_content, Bucket=bucket_name, Key=image_s3_path)
                image_s3_link = f's3://{bucket_name}/{image_s3_path}'
            except Exception as e:
                print(f"Error uploading image for {pub['Title']}: {e}")

        # Append data for Snowflake insertion
        publications_data.append({
            "Title": pub["Title"],
            "Summary": summary,
            "PDF Link": pdf_s3_link,
            "Image Link": image_s3_link
        })

driver.quit()
print("Data extraction and S3 upload complete.")

    
    # Push to XCom for the next Airflow task
    

    

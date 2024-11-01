import os
import requests
import boto3
import pandas as pd

# Initialize the S3 client
s3 = boto3.client('s3')

def upload_to_s3(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket"""
    if object_name is None:
        object_name = os.path.basename(file_name)
    
    try:
        s3.upload_file(file_name, bucket, object_name)
        print(f"File uploaded successfully: {object_name}")
    except Exception as e:
        print(f"Error uploading {file_name}: {e}")

def download_file(url, local_filename):
    """Download a file from a URL and save it locally"""
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(local_filename, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded: {local_filename}")
            return local_filename
        else:
            print(f"Failed to download file from {url}")
            return None
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return None

# S3 bucket name
bucket_name = 'bigdata7245'

# CSV Files
pdf_csv_file = '/Users/asihwaryapatil/Downloads/data_ingestion/merged_publications_data.csv'  # CSV with Title, Summary, PDF Link
image_csv_file = '/Users/asihwaryapatil/Downloads/data_ingestion/merged_publications_data.csv'  # CSV with Title, Image URL

# Read CSV files
pdf_data = pd.read_csv(pdf_csv_file)
image_data = pd.read_csv(image_csv_file)

# Process and upload PDF files
for index, row in pdf_data.iterrows():
    pdf_link = row['PDF Link']  # Accessing the 'PDF Link' column
    title = row['Title']

    # Define local file path
    local_pdf = f"/tmp/{title}.pdf"
    
    # Download the PDF from the URL
    downloaded_pdf = download_file(pdf_link, local_pdf)

    # If download was successful, upload it to S3
    if downloaded_pdf:
        upload_to_s3(downloaded_pdf, bucket_name, f'staging/pdfs/{title}.pdf')

# Process and upload image files
for index, row in image_data.iterrows():
    image_link = row['Image URL']  # Accessing the 'Image URL' column
    title = row['Title']

    # Check if the URL is valid and not a placeholder
    if image_link and "Image not found" not in image_link:
        # Define local file path
        local_image = f"/tmp/{title}.jpg"
        
        # Download the image from the URL
        downloaded_image = download_file(image_link, local_image)

        # If download was successful, upload it to S3
        if downloaded_image:
            upload_to_s3(downloaded_image, bucket_name, f'staging/images/{title}.jpg')
    else:
        print(f"Invalid image URL for title '{title}': {image_link}")

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import pandas as pd
import time

# Set up Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run in headless mode

# Initialize WebDriver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# Base URL for publication pages
base_url = "https://rpc.cfainstitute.org/en/research-foundation/publications#"
publications_try_data = []

# Loop through each page (first 10 pages)
for page in range(10):
    # Construct the URL for the current page
    if page == 0:
        current_page_url = base_url + "sort=%40officialz32xdate%20descending"
    else:
        current_page_url = f"{base_url}first={page * 10}&sort=%40officialz32xdate%20descending"
    # Open the current page
    driver.get(current_page_url)
    time.sleep(3)  # Wait for content to load

    # List to hold temporary publication data from main page
    main_page_publications = []

    # Find and store publication details on the main page
    publications = driver.find_elements(By.CSS_SELECTOR, 'div.coveo-result-row')
    for publication in publications:
        try:
            title = publication.find_element(By.CSS_SELECTOR, 'h4.coveo-title a').text.strip()
            publication_url = publication.find_element(By.CSS_SELECTOR, 'h4.coveo-title a').get_attribute('href')
            
            # Attempt to get the image URL, use "Image not found" if it fails
            try:
                image_url = publication.find_element(By.CSS_SELECTOR, 'img.coveo-result-image').get_attribute('src')
            except:
                image_url = "Image not found"

        except Exception as e:
            title = "Title not found"
            publication_url = None
            image_url = "Image not found"
        
        # Store main page details in a list if the publication URL is unique
        if publication_url and not any(d["Publication URL"] == publication_url for d in main_page_publications):
            main_page_publications.append({
                "Title": title,
                "Publication URL": publication_url,
                "Image URL": image_url
            })

    # Loop through the publications collected from the main page
    for pub in main_page_publications:
        print(f"Visiting publication: {pub['Title']} - {pub['Publication URL']}")
        
        # Visit the publication's detail page
        driver.get(pub["Publication URL"])
        time.sleep(3)

        # Extract additional details from the publication's page
        try:
            # Try extracting summary from the more specific 'article__paragraph' structure
            summary_elements = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.article__paragraph p'))
            )
        except:
            # If that doesn't exist, fall back to extracting from a more generic structure
            try:
                summary_elements = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div p'))
                )
            except:
                summary_elements = []

        # Clean up and extract summary text
        if summary_elements:
            summary = ' '.join([para.text.strip() for para in summary_elements if para.text.strip()])
            summary = summary.replace("\xa0", " ").strip()  # Handle non-breaking spaces
        else:
            summary = "No summary found"

        try:
            pdf_link_element = driver.find_element(By.CSS_SELECTOR, 'a.content-asset.content-asset--primary')
            pdf_link = pdf_link_element.get_attribute('href')
        except:
            pdf_link = "No PDF link found"

        # Append all data for the publication if it's not already in publications_try_data
        # Ensure the script doesn't crash if a dictionary lacks "Publication URL"
        if not any(d.get("Publication URL") == pub["Publication URL"] for d in publications_try_data):
            publications_try_data.append({
                "Title": pub["Title"],
                "Summary": summary,
                "PDF Link": pdf_link,
                "Image URL": pub["Image URL"],
            })

# Convert the data to a DataFrame
publications_df = pd.DataFrame(publications_try_data)
# Save the DataFrame to a CSV file
publications_df.to_csv('merged_publications_data.csv', index=False)
# Close the browser
driver.quit()
print("Data extraction complete. Data saved to 'merged_publications_data.csv'.")

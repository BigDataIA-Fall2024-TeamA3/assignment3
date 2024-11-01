# Assignment
https://github.com/BigDataIA-Fall2024-TeamA3/assignment3.git
https://codelabs-preview.appspot.com/?file_id=1108f_RVuEkfsVlGJQGJEQ4WrKFofZ4L3zuNXTXI-Yfc

## Introduction

The project focuses on building an automated pipeline to ingest and manage research publications from the CFA Institute Research Foundation. To make the data accessible and interactive, a client-facing application is developed using FastAPI and Streamlit, with NVIDIA-powered resources for real-time document summaries and a multi-modal Retrieval-Augmented Generation (RAG) model for advanced question answering. 

## Proof of Concept

This project integrates several key technologies, each chosen for its strengths in automation, data handling, or user interactivity. The following tools are used:

- **Selenium** for web scraping to extract publication details such as titles, summaries, images, and PDFs.
- **AWS S3** for scalable storage of scraped files using the **Boto3** library.
- **Snowflake** for structured metadata storage, connected via the Snowflake Connector.
- **Airflow** for orchestrating the entire data pipeline.
- **NVIDIA’s RAG model** for document interaction and advanced query capabilities, with embeddings stored in **Milvus** for efficient search and retrieval.
- **FastAPI** as the backend API layer for user interactions, with Docker for deployment consistency.

Challenges and Solutions
Latency in Query Processing: Configuring NVIDIA’s model for optimized chunking and embedding caching reduced processing time.
Efficient Document Retrieval: Using Milvus for vector storage accelerated RAG-based querying.
Scalability and Deployment: Containerization with Docker ensures consistent deployment across environments.

Prerequisites
Python 3.8 or higher
Docker
AWS Account (for S3)
Snowflake Account
NVIDIA API key for meta/llama-3.1-70b-instruct

## Getting Started:
step 1: Installation
git clone https://github.com/BigDataIA-Fall2024-TeamA3/assignment3.git


step2: Create env file:

AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=
S3_BUCKET_NAME=

NVIDIA_API_KEY=

SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=


step3:
Run Docker containers:
docker-compose up

## Declaration
WE ATTEST THAT WE HAVEN’T USED ANY OTHER STUDENTS’ WORK IN OUR ASSIGNMENT AND ABIDE BY THE POLICIES LISTED IN THE STUDENT HANDBOOK

Contribution:
a. Aishwarya Patil: 33%

b. Deepak Kumar: 33%

c. Nivedhithaa govindaraj: 33%

## References: 
https://blog.apify.com/web-scraping-with-selenium-and-python/
https://docs.streamlit.io/
https://fastapi.tiangolo.com/
https://github.com/NVIDIA/GenerativeAIExamples/tree/main/community/llm_video_series/video_2_multimodal-rag
https://github.com/run-llama/llama_parse/blob/main/examples/multimodal/multimodal_report_generation.ipynb
https://docs.nvidia.com/

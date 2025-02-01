# Content Extraction using Open Source and Enterprise Service

## Project Description

In this project, we focus on developing a prototype AI application that extracts, processes, and organizes data from unstructured sources such as PDFs and web pages. The goal is to evaluate the feasibility of using open-source tools versus enterprise solutions for data extraction and transformation.

Technologies Involved
1. PDF Extraction: 
  * Open Source: pymupdf, pdfplumber, Fitz
  * Enterprise: Adobe API Extract
2. Website Scraping:
  * Open Source: BeuatifulSoup, requests
  * Enterprise: Apify

## Architecture Diagram
![data_extraction_diagram](https://github.com/user-attachments/assets/253c875a-afa6-4353-9f5a-04231af16d78)

## Directory Structure

![image](https://github.com/user-attachments/assets/4ec4cc6c-9a9b-40b0-b61a-1cf19900b908)


## Instructions to run this project

1. Clone the Repository by running : git clone https://github.com/madhura-adadande/Big_Data_Content_Extraction.git
2. Set up a Virtual Environment:
   * python -m venv venv
   * venv\Scripts\activate
3. Install Dependencies by running - pip install -r requirements.txt
4. Start the FASTAPI Backend Locally:
   * cd ../fastapi_backend
   * uvicorn main:app --host 0.0.0.0 --port 8000 --reload
5. Start the Streamlit Frontend:
   * cd streamlit_frontend
   * streamlit run app.py
6. Test(Upload and Extract PDFs)
   * Open the Streamlit UI.
   * Select PDF to Markdown.
   * Upload a PDF file.
   * Choose either: Open-Source Extraction, Enterprise Extraction
   * Click Generate Markdown.
   * View extracted content & download images.
7. Test(Extract Website Content)
   * Open the Streamlit UI.
   * Select Website URL to Markdown.
   * Enter the website URL.
   * Choose either: Open-Source Extraction, Enterprise Extraction
   * Click Generate Markdown.
   * View extracted content & download images.
8. Deploy FastAPI on Render:
   * cd fastapi_backend
   * echo "#!/bin/bash" > start.sh
   * echo "uvicorn main:app --host 0.0.0.0 --port 10000" >> start.sh
   * chmod +x start.sh
   * pip freeze > requirements.txt
   * git add .
   * git commit -m "Prepare FastAPI for Render deployment"
   * git push origin main
 9. Deploy to Render
   * ./start.sh
10. Deploy Streamlit Frontend on Render
11. Final Testing
   * Open your Streamlit Web App.
   * Upload a PDF or Enter a Website URL.
   * Verify Markdown extraction and Image Previews.
   * Check if files are correctly uploaded to S3.

## Contributions
- Vemana Anil Kumar - 33.3%
- Ashwin Badamikar - 33.3%
- Madhura Adadande - 33.3%
  
WE ATTEST THAT WE HAVEN’T USED ANY OTHER STUDENTS’ WORK IN OUR ASSIGNMENT AND ABIDE BY THE POLICIES LISTED IN THE STUDENT HANDBOOK


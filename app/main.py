import os
import tempfile
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
import pandas as pd
import PyPDF2
import io
import logging
import requests
from dotenv import load_dotenv


# OpenAI API configuration
load_dotenv(".env")
os.environ["OPENAI_API_KEY"] = str(os.getenv("OPENAI_API_KEY"))

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

OPENAI_API_URL = 'https://api.openai.com/v1/files'




app = FastAPI()
logging.basicConfig(level=logging.DEBUG)

def extract_text_from_xlsx(file):
    df = pd.read_excel(file)
    return df.to_string()

def extract_text_from_csv(file):
    df = pd.read_csv(file)
    return df.to_string()

def extract_text_from_pdf(file):
    pdf_reader = PyPDF2.PdfReader(file)
    text = ""
    for page in pdf_reader.pages:
        text += page.extract_text()
    return text

@app.get("/")
async def read_root():
    return {"message": "Welcome to the file parser API!"}

@app.post("/parse-file")
async def parse_file(request: Request):
    logging.debug(f"Received request: {request.method} {request.url}")
    logging.debug(f"Headers: {request.headers}")
    
    content_type = request.headers.get("Content-Type")
    logging.debug(f"Content-Type: {content_type}")
    
    if content_type != "application/octet-stream":
        return JSONResponse(content={
            "success": 0,
            "raw": "",
            "error": "Invalid Content-Type. Expected application/octet-stream"
        }, status_code=400)
    
    content = await request.body()
    logging.debug(f"Received content length: {len(content)} bytes")
    
    try:
        # Determine file type based on content
        if content.startswith(b"PK"):
            logging.debug("Detected XLSX file")
            text = extract_text_from_xlsx(io.BytesIO(content))
        elif b"%" in content[:4]:
            logging.debug("Detected PDF file")
            text = extract_text_from_pdf(io.BytesIO(content))
        else:
            logging.debug("Assuming CSV file")
            text = extract_text_from_csv(io.BytesIO(content))
        
        return JSONResponse(content={
            "success": 1,
            "raw": text,
            "error": ""
        }, status_code=200)
    except Exception as e:
        logging.error(f"Error processing file: {str(e)}", exc_info=True)
        return JSONResponse(content={
            "success": 0,
            "raw": "",
            "error": str(e)
        }, status_code=500)

@app.post("/upload-file-openai")
async def upload_file_to_openai(request: Request):
    """
    Endpoint to upload a file to OpenAI API
    Expects binary file content and supports PDF, XLSX, CSV
    Returns OpenAI file ID or error
    """
    logging.debug(f"Received request: {request.method} {request.url}")
    
    # Get file content
    content = await request.body()
    logging.debug(f"Received content length: {len(content)} bytes")
    
    temp_file_path = None
    try:
        # Determine file extension based on content
        if content.startswith(b"PK"):
            file_extension = ".xlsx"
        elif b"%" in content[:4]:
            file_extension = ".pdf"
        else:
            file_extension = ".csv"
        
        # Create a temporary file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=file_extension)
        temp_file_path = temp_file.name
        
        try:
            # Write content and close the file
            with open(temp_file_path, 'wb') as f:
                f.write(content)
            
            # Prepare files for OpenAI upload
            with open(temp_file_path, 'rb') as file_to_upload:
                files = {
                    'file': (os.path.basename(temp_file_path), file_to_upload),
                    'purpose': (None, 'assistants')
                }
                
                # Headers for OpenAI API
                headers = {
                    'Authorization': f'Bearer {OPENAI_API_KEY}'
                }
                
                # Make request to OpenAI
                response = requests.post(OPENAI_API_URL, files=files, headers=headers)
        
        finally:
            # Ensure file is closed and deleted
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                except Exception as del_error:
                    logging.error(f"Error deleting temporary file: {del_error}")
        
        # Check OpenAI API response
        if response.status_code == 200:
            file_data = response.json()
            return JSONResponse(content={
                "success": 1,
                "file_id": file_data.get('id'),
                "error": ""
            }, status_code=200)
        else:
            return JSONResponse(content={
                "success": 0,
                "file_id": "",
                "error": response.text
            }, status_code=response.status_code)
    
    except Exception as e:
        # Ensure file is deleted even if an exception occurs
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except Exception as del_error:
                logging.error(f"Error deleting temporary file: {del_error}")
        
        logging.error(f"Error uploading file to OpenAI: {str(e)}", exc_info=True)
        return JSONResponse(content={
            "success": 0,
            "file_id": "",
            "error": str(e)
        }, status_code=500)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
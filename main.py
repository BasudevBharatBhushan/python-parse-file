from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
import pandas as pd
import PyPDF2
import io
import logging

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

@app.post("/parse-file")
@app.post("/parse-file/")
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
from fastapi import FastAPI, UploadFile, File, HTTPException
from typing import List
from fastapi.responses import JSONResponse
from llama_index.core import Settings, VectorStoreIndex, StorageContext
from llama_index.vector_stores.milvus import MilvusVectorStore
from llama_index.core.node_parser import SentenceSplitter
from llama_index.embeddings.nvidia import NVIDIAEmbedding
from llama_index.llms.nvidia import NVIDIA
from pydantic import BaseModel
from typing import List
from services.document_service import get_all_documents
import shutil
import os

from fastapi.middleware.cors import CORSMiddleware

from utils.s3_utils import list_buckets, list_s3_documents, upload_file, download_file, check_connection
from config import fastapi_config # Contains env variables, access by eg: "fastapi_config.AWS_ACCESS_KEY_ID"
from routers import rag
from utils.document_processors import load_multimodal_data, load_data_from_directory
from utils.snowflake_client import SnowflakeClient


from IPython import embed


from pydantic import BaseModel

class DocumentSelection(BaseModel):
    document_id: str


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)


def initialize_settings():
    Settings.embed_model = NVIDIAEmbedding(model="nvidia/nv-embedqa-e5-v5", truncate="END")
    Settings.llm = NVIDIA(model="meta/llama-3.1-70b-instruct")
    Settings.text_splitter = SentenceSplitter(chunk_size=600)

def create_index(documents):
    vector_store = MilvusVectorStore(
            host = "127.0.0.1",
            port = 19530,
            dim = 1024
    )
    storage_context = StorageContext.from_defaults(vector_store=vector_store)
    return VectorStoreIndex.from_documents(documents, storage_context=storage_context)


#pydantic model for query
class QueryRequest(BaseModel):
    query: str

@app.get("/")
async def read_root():
    return {"Hello": "World"}


@app.get("/list_buckets")
async def get_buckets():
    buckets = list_buckets()
    return {"buckets": buckets}

@app.get("/list_documents")
async def get_documents():
    objects = list_s3_documents()
    return {"objects": objects}

@app.get("/get_sf_document_details")
def get_document_details(document_name: str):
    try:
        details = get_document_details(document_name)
        return {"details": details}
    except Exception as e:
        return HTTPException(status_code=404, detail=str(e))
    


@app.get("/get_document_details")
def get_document_details(document_name: str):
    try:
        details = get_document_details(document_name)
        return {"details": details}
    except Exception as e:
        return HTTPException(status_code=404, detail=str(e))

@app.get("/documents", response_model=List[DocumentInfo])
async def list_documents():
    try:
        documents = await get_all_documents()
        return documents
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload")
async def upload_to_s3(file: UploadFile = File(...)):
    try:
        with open(file.filename, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        if upload_file(file.filename, fastapi_config.S3_BUCKET_NAME, file.filename):
            return JSONResponse(content={"message": "File uploaded successfully"}, status_code=200)
        else:
            return JSONResponse(content={"message": "Failed to upload file"}, status_code=500)
    finally:
        if os.path.exists(file.filename):
            os.remove(file.filename)


@app.post("/process_files/")
async def process_files(files: list[UploadFile]):
    # set_environment_variables()
    initialize_settings()
    documents = load_multimodal_data(files)
    index = create_index(documents)
    return JSONResponse(content={"message": "Files processed and index created!"})

@app.post("/process_directory/")
async def process_directory(directory_path: str):
    if not os.path.isdir(directory_path):
        raise HTTPException(status_code=400, detail="Invalid directory path.")
    # set_environment_variables()
    initialize_settings()
    documents = load_data_from_directory(directory_path)
    index = create_index(documents)
    return JSONResponse(content={"message": "Directory processed and index created!"})


@app.post("/query")
async def query_index(request: QueryRequest):
    query = request.query
    # Assuming you have a global index or a way to retrieve the index
    response = st.session_state['index'].as_query_engine(similarity_top_k=20, streaming=True).query(query)
    full_response = ""
    for token in response.response_gen:
        full_response += token
    return JSONResponse(content={"response": full_response})


@app.get("/list_documents_info")
def list_documents_info():
    try:
        snowflake_client = SnowflakeClient()
        df = snowflake_client.fetch_document_info()
        print("list_documents_info:", df)
        snowflake_client.close_connection()
        print("list_documents_info:", df)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


index = None

@app.post("/create_index")
async def create_index_endpoint(directory: str):
    global index
    try:
        index = create_index(directory)
        return {"message": "Index created successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query")
async def query_endpoint(query: str):
    global index
    if not index:
        raise HTTPException(status_code=400, detail="Index not created")
    try:
        response = query_index(index, query)
        return {"response": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
from fastapi import FastAPI
app = FastAPI() # initialize the Fastapi app

import requests
import mimetypes
from urllib.parse import urlparse
import filetype
import logging
import os
import json
import asyncio
from crawl4ai import AsyncWebCrawler
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from fastapi import FastAPI, Form
from fastapi.responses import JSONResponse
import requests
import tempfile
from pathlib import Path
from fastapi import FastAPI, Form
from fastapi.responses import JSONResponse
import requests
import tempfile
from pathlib import Path
from haystack import Pipeline
from haystack.components.writers import DocumentWriter
from haystack.components.preprocessors import DocumentSplitter, DocumentCleaner
from haystack.components.converters import PyPDFToDocument, MarkdownToDocument, TextFileToDocument
from haystack.components.builders import PromptBuilder
from haystack_integrations.document_stores.chroma import ChromaDocumentStore
from haystack_integrations.components.retrievers.chroma import ChromaEmbeddingRetriever
from haystack_integrations.components.embedders.ollama import OllamaTextEmbedder, OllamaDocumentEmbedder
from haystack_integrations.components.generators.ollama import OllamaGenerator
from haystack.components.routers import FileTypeRouter
from haystack.components.joiners import DocumentJoiner
import logging
from pathlib import Path
import logging
import json
from haystack import Pipeline
from haystack.components.writers import DocumentWriter
from haystack_integrations.document_stores.chroma import ChromaDocumentStore
from haystack_integrations.components.embedders.ollama import OllamaDocumentEmbedder, OllamaTextEmbedder
from haystack_integrations.components.retrievers.chroma import ChromaEmbeddingRetriever
from haystack.components.builders import PromptBuilder
from haystack_integrations.components.generators.ollama import OllamaGenerator
from haystack.utils import ByteStream
from haystack.components.converters import JSONConverter
from haystack import Document
documents.append(Document(content=item['content'], meta={"tag": item.get("tag", ""), "url": url}))
app = FastAPI()

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def identify_url(url):
    """
    Identifies the type of content a given URL points to, 
such as an article or a specific file type.

    Parameters:
        url (str): The URL to identify.

    Returns:
        str or None: Returns "article" if the URL points to a web page, 
the file extension if identified,
                     or None if the type cannot be determined.
    """
    try:
        # Perform a HEAD request to get headers without downloading the content
        response = requests.head(url, allow_redirects=True, timeout=5)
        content_type = response.headers.get('Content-Type', '')

        # Check if the URL points to a web page
        if 'text/html' in content_type:
            logger.info("The URL points to a web page or article.")
            return "article"

        # Attempt to determine the file extension from the Content-Type header
        extension = mimetypes.guess_extension(content_type.split(';')[0])
        if not extension:
            # If Content-Type doesn't help, check the URL path for a file extension
            path = urlparse(response.url).path
            extension = mimetypes.guess_extension(mimetypes.guess_type(path)[0] or '')

        if extension:
            logger.info(f"The URL points to a file of type '{extension}'")
            return extension.replace(".", "")  # Remove the leading dot for consistency
        else:
            # Fallback: Download a small chunk and use the 'filetype' library to guess the file type
            response = requests.get(url, stream=True, timeout=5)
            response.raise_for_status()  # Raise an error for HTTP errors
            chunk = response.raw.read(2048)  # Read the first 2KB of the file
            kind = filetype.guess(chunk)
            if kind:
                file_type = kind.extension
                logger.info(f"The URL points to a file of type '{file_type}'")
                return file_type
            else:
                logger.info("Could not determine the file type.")
                return None
    except requests.exceptions.RequestException as e:
        # Handle network-related errors
        logger.info(f"Network error occurred: {e}")
    except Exception as e:
        # Handle any other unexpected errors
        logger.info(f"An error occurred: {e}")
async def extract_tech_content(url):
    """
    Extracts the main content of a webpage, excluding headers and footers, 
using an asynchronous web crawler.

    Parameters:
        url (str): The URL of the webpage to extract content from.

    Returns:
        dict: The extracted content in JSON format.
    """
    # Initialize the asynchronous web crawler with verbose mode enabled for detailed logging 
    async with AsyncWebCrawler(verbose=True) as crawler:
    #Use the crawler's 'arun' method to fetch and extract content
        result = await crawler.arun(url=url, extraction_strategy=LLMExtractionStrategy(
                provider="ollama/llama3.2",  # Specify the LLM provider
                base_url="Your_Ollama_URL",  # Base URL for the LLM API
                instruction="Get me the page content without any header/footer tab data",  # Extraction instructions
                api_token='ollama',  # API token for authentication
            ),
            bypass_cache=True,  # Ensure fresh content by bypassing the cache
        )

    # Parse the extracted content into JSON format
    tech_content = json.loads(result.extracted_content)
    return tech_content
@app.post("/ingest")
async def ingest(url: str = Form(...)):
    try:
        # Identify the type of resource the URL points to
        resource_type = identify_url(url)

        if resource_type == "article":
            # Handle URLs pointing to web articles
            result = await ingest_url(url)
        elif resource_type:
            # Handle URLs pointing to files
            response = requests.get(url, stream=True)
            response.raise_for_status()

            # Create a temporary file to store the downloaded content
            with tempfile.NamedTemporaryFile(delete=False,
 suffix=f".{resource_type}") as temp_file:
                for chunk in response.iter_content(chunk_size=8192): 
 # Read content in chunks
                    if chunk:
                        temp_file.write(chunk)
                temp_file_path = Path(temp_file.name)

            # Process the downloaded file
            result = ingest_file(str(temp_file_path))

            # Delete the temporary file after processing
            temp_file_path.unlink()
        else:
            # Return an error if the URL content type cannot be identified
            return JSONResponse(
                content={"status": "error", 
"message": "Unable to identify the URL content type."},
                status_code=400
            )

        # Return the result of the ingestion process
        if result["status"] == "success":
            return JSONResponse(content=result, status_code=200)
        else:
            return JSONResponse(content=result, status_code=500)
    except Exception as e:
        # Handle unexpected errors
        logger.error(f"An error occurred during ingestion: {str(e)}")
        return JSONResponse(
            content={"status": "error", "message": str(e)},
            status_code=500
        )
@app.post("/generate")
async def generate(query: str = Form(...), url: str = Form(...)):
    """
    Endpoint to generate results based on a query and a URL. 
    The URL can point to an article or a downloadable file.

    Parameters:
        query (str): The query to process with the content.
        url (str): The URL to fetch content from.

    Returns:
        JSONResponse: A JSON response with the generated result or an error message.
    """
    try:
        # Identify the type of content the URL points to
        resource_type = identify_url(url)
        print(resource_type)  # Debug: Log the identified resource type

        if resource_type == "article":
            # Handle URLs pointing to web articles
            result = get_url_result(query)
        elif resource_type:
            # Handle URLs pointing to files
            response = requests.get(url, stream=True)
            response.raise_for_status()

            # Create a temporary file to store the downloaded content
            with tempfile.NamedTemporaryFile(delete=False, 
suffix=f".{resource_type}") as temp_file:
                for chunk in response.iter_content(chunk_size=8192):  # Download file in chunks
                    if chunk:
                        temp_file.write(chunk)
                temp_file_path = Path(temp_file.name)

            # Process the downloaded file
            result = get_file_result(query)

            # Delete the temporary file after processing
            temp_file_path.unlink()
        else:
            # Return an error if the URL content type cannot be identified
            return JSONResponse(
                content={
                    "status": "error",
                    "message": "Unable to identify the URL content type."
                },
                status_code=400
            )

        # Return the result of the generation process
        if result["status"] == "success":
            return JSONResponse(content=result, status_code=200)
        else:
            return JSONResponse(content=result, status_code=500)
    except Exception as e:
        # Handle unexpected errors
        logger.error(f"An error occurred during generation: {str(e)}")
        return JSONResponse(
            content={"status": "error", "message": str(e)},
            status_code=500
        )
    # Confidential credentials and configuration
ollama_url = "Your_Ollama_URL"  # CONFIDENTIAL: Base URL for the Ollama API
llm = "llama3.2"  # Large Language Model being used
embedding_model = "nomic-embed-text"  # Model used for embedding text data
chroma_db_path = "./chroma"  # Path to store Chroma database files on disk

# Prompt template for querying based on the provided context
prompt_template = """
Answer the following query based on the provided context. 
If the context does not include an answer, reply with 
'I don't know, ask about provided data.'.

Query: {{query}}

Documents:
{% for doc in documents %}
    {{ doc.content }}
{% endfor %}

Answer:
"""  
# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def ingest_file(file: str):
    """
    Ingests a file (PDF, text, or markdown) into ChromaDB using Ollama embedding model.

    Parameters:
        file (str): The file path to be ingested.

    Returns:
        dict: A status message indicating success or failure of ingestion.
    """
    try:
        # Create an indexing pipeline
        indexing_pipeline = Pipeline()

        # Add components for file conversion, processing, and storage
        indexing_pipeline.add_component("file_type_router", 
FileTypeRouter(mime_types=["text/plain", "application/pdf", "text/markdown"]))
        indexing_pipeline.add_component("pypdf_converter", PyPDFToDocument())
        indexing_pipeline.add_component("text_file_converter",
 TextFileToDocument())
        indexing_pipeline.add_component("markdown_converter",
 MarkdownToDocument())
        indexing_pipeline.add_component("document_joiner", 
DocumentJoiner())
        indexing_pipeline.add_component("document_cleaner", 
DocumentCleaner())
        indexing_pipeline.add_component("splitter",
 DocumentSplitter(split_by="word", split_length=2000, 
split_overlap=500))
        indexing_pipeline.add_component("embedder",
 OllamaDocumentEmbedder(model=embedding_model, url=ollama_url))
        indexing_pipeline.add_component(
            "writer",
            DocumentWriter(ChromaDocumentStore(persist_path=
chroma_db_path, collection_name='file-index'))
        )

        # Connect components to form the pipeline
        indexing_pipeline.connect("file_type_router.text/plain",
 "text_file_converter.sources")
        indexing_pipeline.connect("file_type_router.application/pdf",
 "pypdf_converter.sources")
        indexing_pipeline.connect("file_type_router.text/markdown",
 "markdown_converter.sources")
        indexing_pipeline.connect("text_file_converter", "document_joiner")
        indexing_pipeline.connect("pypdf_converter", "document_joiner")
        indexing_pipeline.connect("markdown_converter", "document_joiner")
        indexing_pipeline.connect("document_joiner", "document_cleaner")
        indexing_pipeline.connect("document_cleaner", "splitter")
        indexing_pipeline.connect("splitter", "embedder")
        indexing_pipeline.connect("embedder", "writer")

        logger.info("Ingestion pipeline connected!")

        # Run the ingestion pipeline with the provided file
        indexing_pipeline.run({"file_type_router": {"sources": [Path(file)]}})
        logger.info(f"Successfully ingested file: {file}")

        return {"status": "success", "message": f"File {file} ingested successfully"}
    except FileNotFoundError as e:
        logger.error(f"File not found: {str(e)}")
        return {"status": "error", "message": str(e)}
    except Exception as e:
        logger.error(f"An error occurred during ingestion: {str(e)}")
        return {"status": "error", "message": "An unexpected error occurred during ingestion"}

def get_file_result(query: str):
    """
    Processes a query against the ingested documents and retrieves results.

    Parameters:
        query (str): The query to process.

    Returns:
        dict: A status message with the generated result or an error message.
    """
    try:
        # Create a query pipeline
        query_pipeline = Pipeline()

        # Add components for query embedding, retrieval, and generation
        query_pipeline.add_component("text_embedder", 
OllamaTextEmbedder(model=embedding_model, url=ollama_url))
        query_pipeline.add_component(
            "retriever",
            ChromaEmbeddingRetriever(document_store=
ChromaDocumentStore(persist_path=chroma_db_path, collection_name='file-index'))
        )
        query_pipeline.add_component("prompt_builder",
 PromptBuilder(template=prompt_template))
        query_pipeline.add_component("llm",

 OllamaGenerator(model=llm, url=ollama_url))

        # Connect components to form the pipeline
        query_pipeline.connect("text_embedder.embedding",
 "retriever.query_embedding")
        query_pipeline.connect("retriever.documents",
 "prompt_builder.documents")
        query_pipeline.connect("prompt_builder", "llm")

        logger.info("Generation pipeline connected!")

        # Run the query pipeline
        results = query_pipeline.run(
            {
                "text_embedder": {"text": query},
                "prompt_builder": {"query": query},
            }
        )

        # Extract the answer from the results
        answer = results['llm']['replies'][0]
        logger.info(f"Query processed successfully: {query}")

        return {"status": "success", "answer": answer}
    except Exception as e:
        logger.error(f"An error occurred while processing the query: {str(e)}")
        return {"status": "error", "message": "An unexpected error occurred while processing the query"}
    # Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def concatenate_content(data):
    """
    Concatenates all strings in the 'content' list for each dictionary into a single string.

    Parameters:
        data (list): List of dictionaries containing a 'content' key.

    Returns:
        list: Modified list with concatenated content.
    """
    for item in data:
        item['content'] = ' '.join(item['content'])  # Concatenate content strings
    return data

async def get_content(url):
    """
    Extracts and processes content from a URL into Haystack Documents.

    Parameters:
        url (str): The URL to extract content from.

    Returns:
        list: List of Haystack Documents.
    """
    # Extract content using a custom function
    tech_content = await extract_tech_content(url)

    # Uncomment the following lines to use a local JSON file instead
    # with open(r"data/output.json", "r", encoding="utf-8") as f:
    #     tech_content = json.load(f)

    # Process and concatenate the content
    tech_content = concatenate_content(tech_content)
    documents = []

    for item in tech_content:
        item["url"] = url
        source = ByteStream.from_string(json.dumps(item))
        converter = JSONConverter(content_key="content", 
extra_meta_fields=["tag", "url"])
        results = converter.run(sources=[source])
        documents.extend(results["documents"])  # Extend the documents list with results

    return documents

async def ingest_url(url: str):
    """
    Ingests content from a URL into ChromaDB using the Ollama embedding model.

    Parameters:
        url (str): The URL to ingest content from.

    Returns:
        dict: Status message indicating success or failure of ingestion.
    """
    try:
        # Create the ingestion pipeline
        indexing_pipeline = Pipeline()
        indexing_pipeline.add_component("embedder", OllamaDocumentEmbedder(model=embedding_model, url=ollama_url))
        indexing_pipeline.add_component(
            "writer", 
            DocumentWriter(ChromaDocumentStore(persist_path=chroma_db_path, collection_name="url-index"))
        )

        # Connect components
        indexing_pipeline.connect("embedder", "writer")
        logger.info("Ingestion pipeline connected!")

        # Get content from the URL
        documents = await get_content(url)

        # Run the ingestion pipeline
        indexing_pipeline.run({"embedder": {"documents": documents}})
        logger.info(f"Successfully ingested URL content: {url}")

        return {"status": "success", "message": f"URL {url} ingested successfully"}
    except FileNotFoundError as e:
        logger.error(f"File not found: {str(e)}")
        return {"status": "error", "message": str(e)}
    except Exception as e:
        logger.error(f"An error occurred during ingestion: {str(e)}")
        return {"status": "error", "message": "An unexpected error occurred during ingestion"}

def get_url_result(query: str):
    """
    Processes a query against the ingested URL content and retrieves results.

    Parameters:
        query (str): The query to process.

    Returns:
        dict: Status message with the result or an error message.
    """
    try:
        # Create the query pipeline
        query_pipeline = Pipeline()
        query_pipeline.add_component("text_embedder",
 OllamaTextEmbedder(model=embedding_model, url=ollama_url))
        query_pipeline.add_component(
            "retriever", 
            ChromaEmbeddingRetriever(document_store=
ChromaDocumentStore(persist_path=chroma_db_path, collection_name="url-index"))
        )
        query_pipeline.add_component("prompt_builder", 
PromptBuilder(template=prompt_template))
        query_pipeline.add_component("llm",
 OllamaGenerator(model=llm, url=ollama_url))

        # Connect components
        query_pipeline.connect("text_embedder.embedding", 

"retriever.query_embedding")
        query_pipeline.connect("retriever.documents", 
"prompt_builder.documents")
        query_pipeline.connect("prompt_builder", "llm")
        logger.info("Query pipeline connected!")

        # Run the query pipeline
        results = query_pipeline.run(
            {
                "text_embedder": {"text": query},
                "prompt_builder": {"query": query},
            }
        )

        # Extract and return the result
        answer = results['llm']['replies'][0]
        logger.info(f"Query processed successfully: {query}")

        return {"status": "success", "answer": answer}
    except Exception as e:
        logger.error(f"An error occurred while processing the query: {str(e)}")
        return {"status": "error", "message": "An unexpected error occurred while processing the query"}
from model import get_embedding

import psycopg2
import requests
import json

from config import PGDATABASE, PGUSER, PGPASSWORD, PGHOST

OLLAMA_EMBED_URL = "http://localhost:11434/api/embeddings"
OLLAMA_CHAT_URL = "http://localhost:11434/api/chat"

# Choose the Ollama model used in model.py
MODEL_NAME = ""  # e.g. "smollm2:135m" 

def fetch_similar_rows(query_embedding, top_k=5):
    """
    Given a query embedding vector, searches the imdb_staging table for the
    top_k most semantically similar movie overviews using pgvector's
    L2 distance operator (<->). Experiment with other operators.
    """

    # TODO: Implement this function
    pass


def generate_response(prompt):
    """
    Sends the given prompt to the Ollama Chat API and returns the
    generated text response.
    """

    # TODO: Implement this function
    pass


def rag_query(user_query):
    query_embedding = get_embedding(user_query)
    
    similar_overviews = fetch_similar_rows(query_embedding, top_k=3)
    
    context = "\n\n".join(similar_overviews)

    prompt = f"Based on the following movies names:\n\n{context}\n\nAnswer the following question:\n{user_query}\nAnswer:"
    
    return generate_response(prompt)


if __name__ == "__main__":
    user_query = input("Enter your query: ")
    answer = rag_query(user_query)
    print("Response:", answer)
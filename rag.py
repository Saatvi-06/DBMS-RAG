from model import get_embedding
import psycopg2
import requests

from config import PGDATABASE, PGUSER, PGPASSWORD, PGHOST

OLLAMA_CHAT_URL = "http://localhost:11434/api/chat"
MODEL_NAME = "smollm2:135m"
def fetch_similar_rows(query_embedding, top_k=5):
    """
    Given a query embedding vector, searches the imdb_staging table for the
    top_k most semantically similar movie overviews using pgvector's
    L2 distance operator (<->). Experiment with other operators.
    """

    conn = psycopg2.connect(
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        host=PGHOST
    )

    cur = conn.cursor()

    vec = "[" + ",".join(map(str, query_embedding)) + "]"

    cur.execute(
        """
        SELECT Series_Title, Genre, Overview, IMDB_Rating
        FROM imdb_staging
        ORDER BY embedding <-> %s::vector
        LIMIT %s;
        """,
        (vec, top_k)
    )

    rows = cur.fetchall()

    cur.close()
    conn.close()

    # ✅ VERY IMPORTANT: return structured semantic text
    return [
        f"Movie name: {t}\tGenre: {g}\t IMDB_Rating: {r}\nOverview/Story of the movie: {o}\n"
        for t, g, o, r in rows
    ]

def generate_response(prompt):
    """
    Sends the given prompt to the Ollama Chat API and returns the
    generated text response.
    """
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {
                "role": "user",
                "content": prompt + "\n\nGive structured movie recommendations."
            }
        ],
        "stream": False
    }

    response = requests.post(OLLAMA_CHAT_URL, json=payload)
    response.raise_for_status()

    data = response.json()
    return data["message"]["content"]


def rag_query(user_query):
    query_embedding = get_embedding(user_query)
    
    similar_overviews = fetch_similar_rows(query_embedding, top_k=3)
    
    context = "\n\n".join(similar_overviews)

    prompt = f"Based on the following movies names:\n\n{context}\n\nAnswer the following question :\"\n{user_query}\"\nAnswer:"
    
    return generate_response(prompt)

if __name__ == "__main__":
    user_query = input("Enter your query: ")
    answer = rag_query(user_query)
    print("Response:\n", answer)
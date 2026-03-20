from model import get_embedding
import psycopg2
import requests

from config import PGDATABASE, PGUSER, PGPASSWORD, PGHOST

OLLAMA_CHAT_URL = "http://localhost:11434/api/chat"
MODEL_NAME = "smollm2:135m"


def fetch_similar_rows(query_embedding, top_k=5):

    conn = psycopg2.connect(
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        host=PGHOST
    )

    cur = conn.cursor()

    vec = "[" + ",".join(map(str, query_embedding)) + "]"

    cur.execute(
        "SELECT overview FROM imdb_staging ORDER BY embedding <-> %s::vector LIMIT %s;",
        (vec, top_k)
    )

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return [r[0] for r in rows]


def generate_response(prompt):

    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "stream": False
    }

    response = requests.post(OLLAMA_CHAT_URL, json=payload)
    response.raise_for_status()

    data = response.json()
    return data["message"]["content"]


def rag_query(user_query):

    q_emb = get_embedding(user_query)

    docs = fetch_similar_rows(q_emb, top_k=5)

    context = "\n\n".join(docs)

    prompt = f"""
You are a movie assistant.

Context movies:
{context}

Answer the question:
{user_query}
"""

    return generate_response(prompt)


if __name__ == "__main__":
    q = input("Enter query: ")
    print(rag_query(q))
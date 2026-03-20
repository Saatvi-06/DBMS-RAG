import psycopg2
import requests

from config import PGDATABASE, PGUSER, PGPASSWORD, PGHOST

OLLAMA_URL = "http://localhost:11434/api/embeddings"
MODEL_NAME = "smollm2:135m"


def get_embedding(text):

    payload = {
        "model": MODEL_NAME,
        "prompt": text
    }

    response = requests.post(OLLAMA_URL, json=payload)
    response.raise_for_status()

    data = response.json()
    return data["embedding"]


def update_embeddings():

    conn = psycopg2.connect(
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        host=PGHOST
    )

    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT ctid, overview FROM imdb_staging;")
    rows = cur.fetchall()

    for ctid, overview in rows:

        emb = get_embedding(overview)

        vec = "[" + ",".join(map(str, emb)) + "]"

        cur.execute(
            "UPDATE imdb_staging SET embedding = %s::vector WHERE ctid = %s;",
            (vec, ctid)
        )

        print("Updated:", ctid)

    cur.close()
    conn.close()


def setup_database():

    conn = psycopg2.connect(
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        host=PGHOST
    )

    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS imdb_staging;")

    cur.execute("""
        CREATE TABLE imdb_staging (
            Poster_Link TEXT,
            Series_Title TEXT,
            Released_Year TEXT,
            Certificate TEXT,
            Runtime TEXT,
            Genre TEXT,
            IMDB_Rating TEXT,
            Overview TEXT,
            Meta_score TEXT,
            Director TEXT,
            Star1 TEXT,
            Star2 TEXT,
            Star3 TEXT,
            Star4 TEXT,
            No_of_Votes TEXT,
            Gross TEXT,
            embedding vector(576)
        );
    """)

    with open("imdb_top_1000.csv") as f:
        cur.copy_expert("""
            COPY imdb_staging
            (Poster_Link, Series_Title, Released_Year, Certificate, Runtime,
             Genre, IMDB_Rating, Overview, Meta_score, Director,
             Star1, Star2, Star3, Star4, No_of_Votes, Gross)
            FROM STDIN WITH CSV HEADER
        """, f)

    cur.close()
    conn.close()


if __name__ == "__main__":
    # first time run this
    # setup_database()

    update_embeddings()
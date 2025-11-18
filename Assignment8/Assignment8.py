from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.models import Variable

import os
import time
import pandas as pd

from sentence_transformers import SentenceTransformer
from pinecone import Pinecone, ServerlessSpec


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DATA_DIR = "/opt/airflow/dags/data"
RAW_FILE = os.path.join(DATA_DIR, "tmdb_5000_movies.csv")
PREPROCESSED_FILE = os.path.join(DATA_DIR, "tmdb_preprocessed.csv")

with DAG(
    dag_id="tmdb_movies_pinecone_search",
    default_args=default_args,
    description="Build a movie search engine using TMDB dataset + Pinecone",
    schedule_interval=timedelta(days=7),
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=["Assignment8", "tmdb", "pinecone", "search-engine"],
) as dag:

    @task
    def get_data_path():
        """Return path to TMDB CSV that is mounted with the DAGs."""
        if not os.path.exists(RAW_FILE):
            raise FileNotFoundError(f"TMDB dataset not found at {RAW_FILE}")
        print(f"Using TMDB dataset at: {RAW_FILE}")
        return RAW_FILE

    @task
    def preprocess_data(data_path: str):
        """Clean and prepare TMDB data for embedding."""
        df = pd.read_csv(data_path)

        # Make sure fields exist and are strings
        df["title"] = df["title"].astype(str).fillna("")
        # TMDB file usually has 'overview' as description text
        if "overview" in df.columns:
            df["overview"] = df["overview"].astype(str).fillna("")
        else:
            df["overview"] = ""

        # Create metadata text we will embed: title + overview
        df["metadata"] = df.apply(
            lambda row: {
                "title": row["title"],
                "overview": row["overview"],
            },
            axis=1,
        )

        # Add unique IDs as strings
        df["id"] = df.reset_index(drop=True).index.astype(str)

        # Save preprocessed data
        os.makedirs(DATA_DIR, exist_ok=True)
        df.to_csv(PREPROCESSED_FILE, index=False)
        print(f"Preprocessed data saved to {PREPROCESSED_FILE}")
        return PREPROCESSED_FILE

    @task
    def create_pinecone_index():
        """Create or reset Pinecone index."""
        # Use a single consistent Airflow Variable name for your API key
        api_key = Variable.get("PineconeAPI")

        pc = Pinecone(api_key=api_key)
        spec = ServerlessSpec(cloud="aws", region="us-east-1")

        index_name = "semantic-movie-search"
        existing_indexes = [idx["name"] for idx in pc.list_indexes()]

        if index_name in existing_indexes:
            pc.delete_index(index_name)

        pc.create_index(
            name=index_name,
            dimension=384,  # all-MiniLM-L6-v2 embeddings size
            metric="dotproduct",
            spec=spec,
        )

        # Wait until the index is ready
        while not pc.describe_index(index_name).status["ready"]:
            print("Waiting for Pinecone index to be ready...")
            time.sleep(1)

        print(f"Pinecone index '{index_name}' created successfully")
        return index_name

    @task
    def generate_embeddings_and_upsert(data_path: str, index_name: str):
        """Generate embeddings for movie metadata and upsert to Pinecone."""
        api_key = Variable.get("PineconeAPI")

        df = pd.read_csv(data_path)

        model = SentenceTransformer("all-MiniLM-L6-v2", device="cpu")

        pc = Pinecone(api_key=api_key)
        index = pc.Index(index_name)

        batch_size = 100
        total = len(df)
        total_batches = (total + batch_size - 1) // batch_size

        for start in range(0, total, batch_size):
            end = start + batch_size
            batch_df = df.iloc[start:end].copy()
            print(f"Processing batch {start//batch_size + 1}/{total_batches}")

            # Metadata column is a string of dict; ensure it's dict
            if isinstance(batch_df["metadata"].iloc[0], str):
                metadata_list = batch_df["metadata"].apply(eval).tolist()
            else:
                metadata_list = batch_df["metadata"].tolist()

            titles = [
                f"{m['title']} {m.get('overview', '')}" for m in metadata_list
            ]
            embeddings = model.encode(titles)

            upsert_items = []
            for i, (_, row) in enumerate(batch_df.iterrows()):
                upsert_items.append(
                    {
                        "id": str(row["id"]),
                        "values": embeddings[i].tolist(),
                        "metadata": metadata_list[i],
                    }
                )

            index.upsert(upsert_items)

        print(f"Successfully upserted {len(df)} movies to Pinecone")
        return index_name

    @task
    def test_search_query(index_name: str):
        """Test search with a sample movie-style query."""
        api_key = Variable.get("PineconeAPI")

        model = SentenceTransformer("all-MiniLM-L6-v2", device="cpu")
        pc = Pinecone(api_key=api_key)
        index = pc.Index(index_name)

        query = "space exploration adventure"
        query_embedding = model.encode(query).tolist()

        results = index.query(
            vector=query_embedding,
            top_k=5,
            include_metadata=True,
        )

        print(f"Search results for query: '{query}'")
        for match in results["matches"]:
            md = match["metadata"]
            title = md.get("title", "")[:80]
            overview = md.get("overview", "")[:120]
            print(
                f"ID: {match['id']}, Score: {match['score']:.4f}, "
                f"Title: {title}..., Overview: {overview}..."
            )

    # Task dependencies
    raw_path = get_data_path()
    preprocessed_path = preprocess_data(raw_path)
    index_name = create_pinecone_index()
    final_index_name = generate_embeddings_and_upsert(preprocessed_path, index_name)
    test_search_query(final_index_name)

import pickle
import os
import numpy as np
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, String
from sentence_transformers import SentenceTransformer

# --- Configuration ---
DB_PATH = "ai_workbench/db/medical_knowledge.db"
DB_URL = f"sqlite:///{DB_PATH}"
EMBEDDINGS_PATH = "ai_workbench/db/icd10_embeddings.pkl"

# Setup SQLAlchemy (Re-defining model here to keep script standalone)
engine = create_engine(DB_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class ICD10Definition(Base):
    __tablename__ = "icd10_definitions"
    code = Column(String, primary_key=True)
    description = Column(String)

def create_embeddings():
    print("--- Starting Embedding Generation ---")
    
    # 1. Load Data
    session = SessionLocal()
    print("Reading ICD-10 codes from database...")
    definitions = session.query(ICD10Definition).all()
    session.close()
    
    if not definitions:
        print("No definitions found in DB. Run ingest_icd10.py first.")
        return

    codes = [d.code for d in definitions]
    descriptions = [d.description for d in definitions]
    print(f"Loaded {len(definitions)} definitions.")

    # 2. Load Model
    print("Loading Embedding Model (all-MiniLM-L6-v2)...")
    # This will download the model on first run
    model = SentenceTransformer('all-MiniLM-L6-v2')
    
    # 3. Encode
    print("Generating Embeddings (this may take a minute)...")
    embeddings = model.encode(descriptions, show_progress_bar=True)
    
    # 4. Save
    print(f"Saving embeddings to {EMBEDDINGS_PATH}...")
    data = {
        "codes": codes,
        "descriptions": descriptions,
        "embeddings": embeddings
    }
    
    with open(EMBEDDINGS_PATH, 'wb') as f:
        pickle.dump(data, f)
        
    print("--- Success! Embeddings saved. ---")

if __name__ == "__main__":
    create_embeddings()


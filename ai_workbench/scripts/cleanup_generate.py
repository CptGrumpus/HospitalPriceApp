import sys
import pickle
import ollama
import json
import re
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, String, Text
from sentence_transformers import SentenceTransformer, util
from tqdm import tqdm

# --- Configuration ---
MAIN_DB_URL = "sqlite:///hospital.db"
EMBEDDINGS_PATH = "ai_workbench/db/icd10_embeddings.pkl"
MODEL_NAME = "llama3.1"

# --- Database Setup ---
engine = create_engine(MAIN_DB_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class CodeDefinition(Base):
    __tablename__ = "code_definitions"
    code = Column(String, primary_key=True)
    generated_title = Column(String)
    generated_description = Column(Text)
    source_text = Column(Text)
    category = Column(String)
    long_description = Column(String) 

# --- AI Resources ---
print("Loading Search Resources...")
search_model = SentenceTransformer('all-MiniLM-L6-v2')
with open(EMBEDDINGS_PATH, 'rb') as f:
    search_data = pickle.load(f)
print("Resources Loaded.")

def get_icd_hints(query, top_k=5):
    if not query: return []
    query_embedding = search_model.encode(query, convert_to_tensor=True)
    corpus_embeddings = search_data['embeddings']
    hits = util.semantic_search(query_embedding, corpus_embeddings, top_k=top_k)[0]
    
    results = []
    for hit in hits:
        idx = hit['corpus_id']
        results.append(f"- {search_data['descriptions'][idx]}")
    return results

def call_llama_json(prompt):
    try:
        response = ollama.chat(model=MODEL_NAME, messages=[
            {'role': 'user', 'content': prompt},
        ], format='json') # Enforce JSON mode
        return response['message']['content']
    except Exception as e:
        print(f"Ollama Error: {e}")
        return None

def cleanup_batch():
    session = SessionLocal()
    
    print("Fetching 'Unknown Procedure' records...")
    # 1. Select ONLY the failed ones
    records = session.query(CodeDefinition).filter(
        CodeDefinition.generated_title == "Unknown Procedure"
    ).all()
    
    print(f"Found {len(records)} records to fix.")
    
    processed_count = 0
    
    for record in tqdm(records):
        source_text = record.source_text
        if not source_text: source_text = "Medical Procedure"

        # 2. Run AI Pipeline (JSON Mode)
        hints = get_icd_hints(source_text)
        hints_str = "\n".join(hints)
        
        prompt = f"""
        You are an expert medical editor.
        
        TASK: Rewrite this medical procedure into a clean Title and a Patient-Friendly Description.
        
        INPUT: "{source_text}"
        
        CONTEXT CLUES:
        {hints_str}
        
        INSTRUCTIONS:
        1. **title:** Create a clean, Title-Cased Headline (3-7 words). Remove codes like 'CPT'.
        2. **description:** Write 1-2 sentences explaining what happens to the user ('You'). Simple English.
        
        OUTPUT JSON FORMAT:
        {{
            "title": "Your Title Here",
            "description": "Your Description Here"
        }}
        """
        
        response_json = call_llama_json(prompt)
        if not response_json:
            continue
            
        try:
            data = json.loads(response_json)
            title = data.get("title", "Unknown Procedure")
            desc = data.get("description", "No description available.")
            
            # 3. Update DB
            record.generated_title = title
            record.generated_description = desc
            
            processed_count += 1
        except json.JSONDecodeError:
            print(f"JSON Error for {record.code}")
            continue
        
        # Commit every 10 items
        if processed_count % 10 == 0:
            session.commit()
            
    session.commit()
    session.close()
    print("Cleanup processing complete!")

if __name__ == "__main__":
    cleanup_batch()


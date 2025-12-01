import sys
import os
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

# --- Configuration ---
# We use a separate DB for the "Knowledge Base"
DB_PATH = "ai_workbench/db/medical_knowledge.db"
DB_URL = f"sqlite:///{DB_PATH}"

# Setup SQLAlchemy
engine = create_engine(DB_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# --- Database Model ---
class ICD10Definition(Base):
    __tablename__ = "icd10_definitions"
    
    code = Column(String, primary_key=True, index=True)
    description = Column(String)

def init_db():
    print(f"--- Initializing Knowledge DB at {DB_PATH} ---")
    # Ensure the directory exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    Base.metadata.create_all(bind=engine)

def ingest_icd10(file_path):
    print(f"--- Ingesting ICD-10-PCS from: {file_path} ---")
    
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return

    init_db()
    session = SessionLocal()
    
    count = 0
    batch_size = 10000
    batch = []

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                # The file format is usually: CODE (space) DESCRIPTION
                # Example: 0PHB04Z Insertion of Internal Fixation Device...
                # We split on the FIRST space only.
                parts = line.split(' ', 1)
                
                if len(parts) < 2:
                    continue
                    
                code = parts[0].strip()
                desc = parts[1].strip()
                
                # Create object (don't add to session yet, we batch insert)
                def_entry = ICD10Definition(code=code, description=desc)
                batch.append(def_entry)
                
                count += 1
                
                # Bulk insert for speed
                if len(batch) >= batch_size:
                    session.bulk_save_objects(batch)
                    session.commit()
                    batch = []
                    print(f"Processed {count} codes...")

        # Insert remaining
        if batch:
            session.bulk_save_objects(batch)
            session.commit()
            
        print(f"--- Success! Total ICD-10-PCS Codes Ingested: {count} ---")
        
    except Exception as e:
        print(f"Error during ingestion: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    # Default location based on user info
    default_path = "data/raw/icd10pcs_codes_2025.txt"
    
    if len(sys.argv) > 1:
        target_file = sys.argv[1]
    else:
        target_file = default_path
        
    ingest_icd10(target_file)


import sys
import os
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

# Define the database file (must match the main app)
DB_URL = "sqlite:///hospital.db"
engine = create_engine(DB_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# Define the CodeDefinition model
class CodeDefinition(Base):
    __tablename__ = "code_definitions"
    
    code = Column(String, primary_key=True, index=True)
    long_description = Column(String)
    short_description = Column(String)

def init_definitions_table():
    print("--- Creating Code Definitions Table ---")
    Base.metadata.create_all(bind=engine)

def ingest_hcpcs_definitions(file_path):
    print(f"--- Ingesting HCPCS Definitions from: {file_path} ---")
    
    init_definitions_table()
    session = SessionLocal()
    
    try:
        # Read the fixed-width text file
        # Layout based on HCPC2026_recordlayout.txt:
        # Code: 1-5 (0-5 in slice)
        # Long Desc: 12-91 (11-91 in slice)
        # Short Desc: 92-119 (91-119 in slice)
        
        count = 0
        current_code = None
        current_long_desc_parts = []
        current_short_desc = None

        def save_current_code():
            nonlocal count
            if current_code:
                full_long_desc = " ".join(current_long_desc_parts)
                def_entry = CodeDefinition(
                    code=current_code,
                    long_description=full_long_desc,
                    short_description=current_short_desc
                )
                session.merge(def_entry)
                count += 1
                if count % 1000 == 0:
                    session.commit()
                    print(f"Processed {count} definitions...")

        with open(file_path, 'r', encoding='iso-8859-1') as f:
            for line in f:
                # Skip empty lines
                if not line.strip():
                    continue
                
                # Extract fields
                code = line[0:5].strip()
                long_desc_chunk = line[11:91].strip()
                short_desc_chunk = line[91:119].strip()
                
                # Skip if code is empty
                if not code:
                    continue
                
                if code != current_code:
                    # Save previous if exists
                    save_current_code()
                    
                    # Start new
                    current_code = code
                    current_long_desc_parts = [long_desc_chunk] if long_desc_chunk else []
                    current_short_desc = short_desc_chunk
                else:
                    # Continuation
                    if long_desc_chunk:
                        current_long_desc_parts.append(long_desc_chunk)
                    # Usually short_desc is only on first line, so we keep the first one
        
        # Save final entry
        save_current_code()
        session.commit()
        print(f"--- Completed. Total Definitions: {count} ---")
        
    except Exception as e:
        print(f"Error: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    # Default path if not provided
    default_path = "data/raw/hcpc2026_jan_anweb/HCPC2026_JAN_ANWEB.txt"
    
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    elif os.path.exists(default_path):
        file_path = default_path
    else:
        print(f"File not found at default: {default_path}")
        sys.exit(1)
        
    ingest_hcpcs_definitions(file_path)


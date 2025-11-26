from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# Define the database file (local SQLite for now)
DB_URL = "sqlite:///hospital.db"

engine = create_engine(DB_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class Item(Base):
    __tablename__ = "items"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, index=True)  # e.g., "99213"
    code_type = Column(String)         # e.g., "CPT", "HCPCS", "DRG"
    description = Column(String)       # e.g., "Office Visit Level 3"
    hospital_id = Column(String)       # To track which hospital this came from
    setting = Column(String)           # e.g., "inpatient", "outpatient", "facility"

    # Relationship to prices
    prices = relationship("Price", back_populates="item", cascade="all, delete-orphan")

class Price(Base):
    __tablename__ = "prices"

    id = Column(Integer, primary_key=True, index=True)
    item_id = Column(Integer, ForeignKey("items.id"))
    
    payer = Column(String, index=True) # e.g., "Aetna", "Cash", "Gross"
    plan = Column(String)              # e.g., "PPO", "HMO" (optional detail)
    amount = Column(Float)             # The actual price
    notes = Column(String)             # For storing formulas or special pricing logic

    # Relationship back to item
    item = relationship("Item", back_populates="prices")

class CodeDefinition(Base):
    __tablename__ = "code_definitions"
    
    code = Column(String, primary_key=True, index=True)
    long_description = Column(String)
    short_description = Column(String)

def init_db():
    """Creates the tables in the database if they don't exist."""
    print("--- Creating Database Tables ---")
    Base.metadata.create_all(bind=engine)
    print("--- Tables Created Successfully ---")

if __name__ == "__main__":
    init_db()

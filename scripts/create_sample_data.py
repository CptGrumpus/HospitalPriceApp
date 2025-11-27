import sys
sys.path.insert(0, '.')

from src.database import SessionLocal, Item, Price, CodeDefinition, Base, engine

Base.metadata.create_all(bind=engine)

db = SessionLocal()

db.query(Price).delete()
db.query(Item).delete()
db.query(CodeDefinition).delete()
db.commit()

HOSPITALS = ["UOFM", "BEAUMONT", "CHILDRENS", "HENRYFORD"]

PAYERS = [
    ("DISCOUNTED_CASH", None),
    ("Aetna", "PPO"),
    ("Aetna", "HMO"),
    ("Blue Cross Blue Shield", "PPO"),
    ("Blue Cross Blue Shield", "HMO"),
    ("United Healthcare", "Choice Plus"),
    ("Cigna", "Open Access"),
    ("Medicare", None),
    ("Medicaid", None),
    ("Priority Health", "HMO"),
]

PROCEDURES = [
    ("99213", "CPT", "Office Visit - Established Patient, Level 3", "outpatient", 150),
    ("99214", "CPT", "Office Visit - Established Patient, Level 4", "outpatient", 225),
    ("99215", "CPT", "Office Visit - Established Patient, Level 5", "outpatient", 350),
    ("99203", "CPT", "Office Visit - New Patient, Level 3", "outpatient", 200),
    ("99204", "CPT", "Office Visit - New Patient, Level 4", "outpatient", 300),
    ("99205", "CPT", "Office Visit - New Patient, Level 5", "outpatient", 425),
    
    ("70553", "CPT", "MRI Brain with and without Contrast", "outpatient", 2800),
    ("70551", "CPT", "MRI Brain without Contrast", "outpatient", 2200),
    ("72148", "CPT", "MRI Lumbar Spine without Contrast", "outpatient", 2400),
    ("72141", "CPT", "MRI Cervical Spine without Contrast", "outpatient", 2350),
    ("73721", "CPT", "MRI Lower Extremity Joint without Contrast", "outpatient", 1800),
    ("73221", "CPT", "MRI Upper Extremity Joint without Contrast", "outpatient", 1750),
    
    ("74176", "CPT", "CT Abdomen and Pelvis without Contrast", "outpatient", 1200),
    ("74177", "CPT", "CT Abdomen and Pelvis with Contrast", "outpatient", 1500),
    ("71250", "CPT", "CT Chest without Contrast", "outpatient", 950),
    ("71260", "CPT", "CT Chest with Contrast", "outpatient", 1150),
    ("70450", "CPT", "CT Head without Contrast", "outpatient", 850),
    ("70460", "CPT", "CT Head with Contrast", "outpatient", 1050),
    
    ("71046", "CPT", "X-Ray Chest, 2 Views", "outpatient", 125),
    ("73030", "CPT", "X-Ray Shoulder, 2 Views", "outpatient", 95),
    ("73562", "CPT", "X-Ray Knee, 3 Views", "outpatient", 110),
    ("73610", "CPT", "X-Ray Ankle, 3 Views", "outpatient", 105),
    ("73130", "CPT", "X-Ray Hand, 2 Views", "outpatient", 85),
    ("72100", "CPT", "X-Ray Spine Lumbosacral, 2-3 Views", "outpatient", 135),
    
    ("76856", "CPT", "Ultrasound Pelvic Complete", "outpatient", 425),
    ("76700", "CPT", "Ultrasound Abdomen Complete", "outpatient", 450),
    ("93306", "CPT", "Echocardiogram Complete", "outpatient", 875),
    ("76805", "CPT", "Ultrasound Obstetric", "outpatient", 375),
    
    ("43239", "CPT", "Upper GI Endoscopy with Biopsy", "outpatient", 1850),
    ("45380", "CPT", "Colonoscopy with Biopsy", "outpatient", 2200),
    ("45378", "CPT", "Colonoscopy Diagnostic", "outpatient", 1950),
    ("49505", "CPT", "Inguinal Hernia Repair", "outpatient", 4500),
    ("47562", "CPT", "Laparoscopic Cholecystectomy", "outpatient", 8500),
    
    ("27447", "CPT", "Total Knee Replacement", "inpatient", 32000),
    ("27130", "CPT", "Total Hip Replacement", "inpatient", 35000),
    ("63030", "CPT", "Lumbar Discectomy", "inpatient", 18000),
    ("22551", "CPT", "Cervical Spine Fusion", "inpatient", 42000),
    ("33533", "CPT", "Coronary Artery Bypass, Single Graft", "inpatient", 85000),
    
    ("99281", "CPT", "Emergency Department Visit, Level 1", "outpatient", 250),
    ("99282", "CPT", "Emergency Department Visit, Level 2", "outpatient", 450),
    ("99283", "CPT", "Emergency Department Visit, Level 3", "outpatient", 750),
    ("99284", "CPT", "Emergency Department Visit, Level 4", "outpatient", 1200),
    ("99285", "CPT", "Emergency Department Visit, Level 5", "outpatient", 1800),
    
    ("80053", "CPT", "Comprehensive Metabolic Panel", "outpatient", 45),
    ("85025", "CPT", "Complete Blood Count (CBC) with Differential", "outpatient", 25),
    ("80061", "CPT", "Lipid Panel", "outpatient", 35),
    ("84443", "CPT", "TSH (Thyroid Stimulating Hormone)", "outpatient", 55),
    ("82947", "CPT", "Glucose Blood Test", "outpatient", 15),
    ("81001", "CPT", "Urinalysis with Microscopy", "outpatient", 20),
    ("87086", "CPT", "Urine Culture", "outpatient", 45),
    
    ("90834", "CPT", "Psychotherapy, 45 minutes", "outpatient", 175),
    ("90837", "CPT", "Psychotherapy, 60 minutes", "outpatient", 225),
    ("96372", "CPT", "Therapeutic Injection", "outpatient", 85),
    ("97110", "CPT", "Physical Therapy - Therapeutic Exercises", "outpatient", 75),
    ("97140", "CPT", "Manual Therapy Techniques", "outpatient", 80),
    
    ("59400", "CPT", "Routine Obstetric Care - Vaginal Delivery", "inpatient", 4500),
    ("59510", "CPT", "Routine Obstetric Care - Cesarean Delivery", "inpatient", 6500),
    ("59025", "CPT", "Fetal Non-Stress Test", "outpatient", 225),
    
    ("G0008", "HCPCS", "Administration of Influenza Virus Vaccine", "outpatient", 25),
    ("G0009", "HCPCS", "Administration of Pneumococcal Vaccine", "outpatient", 25),
    ("J7030", "HCPCS", "Normal Saline Solution Infusion", "outpatient", 15),
    ("J3490", "HCPCS", "Unclassified Drug Injection", "outpatient", 150),
    ("A4550", "HCPCS", "Surgical Trays", "outpatient", 85),
    
    ("470", "DRG", "Major Hip and Knee Joint Replacement", "inpatient", 28000),
    ("743", "DRG", "Uterine and Adnexa Procedures", "inpatient", 12000),
    ("766", "DRG", "Cesarean Section without Complications", "inpatient", 9500),
    ("775", "DRG", "Vaginal Delivery without Complications", "inpatient", 6500),
    ("871", "DRG", "Septicemia without MV >96 Hours", "inpatient", 15000),
    ("392", "DRG", "Esophagitis and Gastroenteritis", "inpatient", 7500),
    ("690", "DRG", "Kidney and Urinary Tract Infections", "inpatient", 8000),
    ("291", "DRG", "Heart Failure and Shock", "inpatient", 12500),
    ("065", "DRG", "Intracranial Hemorrhage or Cerebral Infarction", "inpatient", 18000),
    ("194", "DRG", "Simple Pneumonia", "inpatient", 9000),
    
    ("29881", "CPT", "Knee Arthroscopy with Meniscectomy", "outpatient", 5500),
    ("29827", "CPT", "Shoulder Arthroscopy with Rotator Cuff Repair", "outpatient", 8500),
    ("64483", "CPT", "Epidural Steroid Injection - Lumbar", "outpatient", 1200),
    ("20610", "CPT", "Joint Injection - Major Joint", "outpatient", 350),
    ("11042", "CPT", "Wound Debridement", "outpatient", 450),
    ("12001", "CPT", "Simple Laceration Repair", "outpatient", 275),
    ("10060", "CPT", "Incision and Drainage of Abscess", "outpatient", 425),
    
    ("92014", "CPT", "Comprehensive Eye Exam - Established Patient", "outpatient", 185),
    ("92004", "CPT", "Comprehensive Eye Exam - New Patient", "outpatient", 225),
    ("66984", "CPT", "Cataract Surgery with IOL", "outpatient", 3500),
    
    ("69436", "CPT", "Tympanostomy Tube Insertion", "outpatient", 1800),
    ("42820", "CPT", "Tonsillectomy and Adenoidectomy", "outpatient", 4200),
    
    ("36415", "CPT", "Venipuncture for Blood Draw", "outpatient", 15),
    ("99381", "CPT", "Preventive Visit - Infant (new)", "outpatient", 225),
    ("99391", "CPT", "Preventive Visit - Infant (established)", "outpatient", 175),
    ("99385", "CPT", "Preventive Visit - 18-39 years (new)", "outpatient", 275),
    ("99395", "CPT", "Preventive Visit - 18-39 years (established)", "outpatient", 225),
    ("99396", "CPT", "Preventive Visit - 40-64 years (established)", "outpatient", 250),
    ("99397", "CPT", "Preventive Visit - 65+ years (established)", "outpatient", 275),
]

CODE_DEFINITIONS = [
    ("99213", "Office or other outpatient visit for the evaluation and management of an established patient", "Office visit est patient 15-29 min"),
    ("99214", "Office or other outpatient visit for the evaluation and management of an established patient, moderate complexity", "Office visit est patient 30-39 min"),
    ("70553", "Magnetic resonance imaging, brain, including brain stem; without contrast material, followed by contrast material(s) and further sequences", "MRI brain w/wo contrast"),
    ("74177", "Computed tomography, abdomen and pelvis; with contrast material(s)", "CT abd/pelvis with contrast"),
    ("45380", "Colonoscopy, flexible; with biopsy, single or multiple", "Colonoscopy with biopsy"),
    ("27447", "Arthroplasty, knee, condyle and plateau; medial AND lateral compartments with or without patella resurfacing", "Total knee replacement"),
    ("27130", "Arthroplasty, acetabular and proximal femoral prosthetic replacement (total hip arthroplasty)", "Total hip replacement"),
    ("80053", "Comprehensive metabolic panel", "Comprehensive metabolic panel"),
    ("85025", "Blood count; complete (CBC), automated and automated differential WBC count", "CBC with auto diff"),
]

import random

random.seed(42)

PRICE_VARIANCE = {
    "UOFM": 1.15,
    "BEAUMONT": 1.0,
    "CHILDRENS": 1.25,
    "HENRYFORD": 0.95,
}

PAYER_DISCOUNTS = {
    "DISCOUNTED_CASH": 0.55,
    "Medicare": 0.35,
    "Medicaid": 0.30,
    "Aetna": 0.65,
    "Blue Cross Blue Shield": 0.70,
    "United Healthcare": 0.68,
    "Cigna": 0.67,
    "Priority Health": 0.62,
}

print("Creating sample data...")

for code, long_desc, short_desc in CODE_DEFINITIONS:
    code_def = CodeDefinition(
        code=code,
        long_description=long_desc,
        short_description=short_desc
    )
    db.add(code_def)

for hospital in HOSPITALS:
    variance = PRICE_VARIANCE[hospital]
    
    for code, code_type, description, setting, base_price in PROCEDURES:
        if hospital == "CHILDRENS" and setting == "inpatient" and code_type == "DRG":
            if code in ["766", "775", "59400", "59510"]:
                continue
        
        adjusted_base = base_price * variance * random.uniform(0.92, 1.08)
        
        item = Item(
            code=code,
            code_type=code_type,
            description=description,
            hospital_id=hospital,
            setting=setting
        )
        db.add(item)
        db.flush()
        
        for payer_name, plan in PAYERS:
            base_discount = PAYER_DISCOUNTS.get(payer_name, 0.65)
            final_discount = base_discount * random.uniform(0.95, 1.05)
            
            if payer_name == "DISCOUNTED_CASH":
                amount = round(adjusted_base * final_discount, 2)
            else:
                amount = round(adjusted_base * final_discount, 2)
            
            notes = None
            if code_type == "DRG" and payer_name not in ["DISCOUNTED_CASH", "Medicare", "Medicaid"]:
                if random.random() < 0.3:
                    notes = "Per diem rate may apply for extended stays"
            
            if payer_name == "DISCOUNTED_CASH" and base_price > 5000:
                if random.random() < 0.2:
                    notes = "Payment plan available"
            
            price = Price(
                item_id=item.id,
                payer=payer_name,
                plan=plan,
                amount=amount,
                notes=notes
            )
            db.add(price)

db.commit()

item_count = db.query(Item).count()
price_count = db.query(Price).count()
code_def_count = db.query(CodeDefinition).count()

print(f"Sample data created successfully!")
print(f"  - Items: {item_count}")
print(f"  - Prices: {price_count}")
print(f"  - Code Definitions: {code_def_count}")

db.close()

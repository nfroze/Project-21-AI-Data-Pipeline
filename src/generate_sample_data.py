import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

# REAL-looking company names (will be sanitised later)
COMPANIES = {
    'Acme Industrial Corp': ['North Pipeline', 'South Pipeline', 'East Pipeline'],
    'Global Energy Solutions': ['Offshore Platform A', 'Offshore Platform B', 'Terminal Alpha'],
    'TechFlow Industries': ['Main Distribution Line', 'Secondary Route', 'Backup System'],
    'Premier Oil & Gas': ['Continental Pipeline', 'Regional Network', 'Export Terminal'],
    'United Manufacturing': ['Production Line 1', 'Production Line 2', 'Quality Check System']
}

def generate_pipeline_data(num_records=100):
    """Generate realistic pipeline inspection data matching ROSEN's data types"""
    
    data = []
    
    for _ in range(num_records):
        company = random.choice(list(COMPANIES.keys()))
        pipeline = random.choice(COMPANIES[company])
        
        # Calculate metal loss
        nominal_thickness = round(random.uniform(10.0, 27.0), 2)
        actual_thickness = round(nominal_thickness - random.uniform(0.0, 8.0), 2)
        metal_loss_mm = round(nominal_thickness - actual_thickness, 2)
        metal_loss_percent = round((metal_loss_mm / nominal_thickness) * 100, 1) if nominal_thickness > 0 else 0
        
        record = {
            # Client identification (to be sanitised)
            'client_name': company,
            'pipeline_name': pipeline,
            
            # Inspection metadata
            'inspection_date': fake.date_between(start_date='-2y', end_date='today'),
            'inspection_id': f"INS-{fake.uuid4()[:8].upper()}",
            'inspection_tool': random.choice(['EMAT', 'MFL', 'UT', 'Caliper', 'Combo']),
            
            # Pipeline location
            'pipeline_segment': f"Segment-{random.randint(1, 50)}",
            'girth_weld_location': f"GW-{random.randint(1, 500)}",
            'distance_km': round(random.uniform(0.5, 500.0), 2),
            'latitude': round(random.uniform(25.0, 65.0), 6),
            'longitude': round(random.uniform(-120.0, 60.0), 6),
            
            # Wall thickness measurements (ROSEN's key metrics)
            'wall_thickness_nominal_mm': nominal_thickness,
            'wall_thickness_actual_mm': actual_thickness,
            'metal_loss_mm': metal_loss_mm,
            'metal_loss_percent': metal_loss_percent,
            
            # Defect characterisation
            'defect_type': random.choice(['corrosion', 'crack', 'dent', 'weld_anomaly', 'coating_defect', 'none']),
            'defect_severity': random.choice(['low', 'medium', 'high', 'critical']),
            'defect_length_mm': round(random.uniform(0.0, 200.0), 1) if random.random() > 0.3 else 0.0,
            'defect_width_mm': round(random.uniform(0.0, 100.0), 1) if random.random() > 0.3 else 0.0,
            'defect_depth_mm': round(random.uniform(0.0, 5.0), 2),
            
            # Pressure data
            'operating_pressure_bar': round(random.uniform(20.0, 100.0), 1),
            'maop_bar': round(random.uniform(80.0, 150.0), 1),  # Maximum Allowable Operating Pressure
            
            # Pipeline characteristics
            'pipe_diameter_mm': random.choice([508, 610, 762, 914, 1067]),  # Common sizes
            'pipe_grade': random.choice(['X52', 'X60', 'X65', 'X70', 'X80']),
            
            # Risk assessment
            'risk_level': random.choice(['low', 'medium', 'high', 'critical']),
            'failure_probability': round(random.uniform(0.001, 0.5), 4),
            'recommended_action': random.choice(['monitor', 'schedule_inspection', 'immediate_repair', 'none']),
            'next_inspection_months': random.randint(6, 60),
            
            # Environmental factors
            'soil_type': random.choice(['clay', 'sand', 'rock', 'mixed', 'subsea']),
            'coating_condition': random.choice(['excellent', 'good', 'fair', 'poor', 'failed']),
            'cathodic_protection': random.choice(['adequate', 'marginal', 'inadequate', 'none'])
        }
        
        data.append(record)
    
    return pd.DataFrame(data)

def save_pipeline_data():
    """Generate and save realistic pipeline inspection data"""
    
    print("Generating realistic pipeline inspection data...")
    print("(Using ROSEN-style measurements: wall thickness, metal loss, EMAT/MFL/UT sensors)\n")
    
    df = generate_pipeline_data(100)
    
    # Save raw data
    csv_path = 'data/raw_inspection_data.csv'
    df.to_csv(csv_path, index=False)
    print(f"✓ Saved {len(df)} inspection records to {csv_path}")
    
    # Show sample with key pipeline metrics
    print("\n=== Sample Pipeline Inspection Data ===")
    print(df[['client_name', 'pipeline_name', 'wall_thickness_nominal_mm', 
              'metal_loss_percent', 'defect_type', 'risk_level']].head())
    
    # Show statistics
    print(f"\n=== Data Statistics ===")
    print(f"Companies: {df['client_name'].nunique()}")
    print(f"Critical defects: {len(df[df['risk_level'] == 'critical'])}")
    print(f"Average metal loss: {df['metal_loss_percent'].mean():.1f}%")
    print(f"High severity defects: {len(df[df['defect_severity'] == 'high'])}")
    
    return df

if __name__ == "__main__":
    df = save_pipeline_data()
    print(f"\n✓ Successfully generated pipeline inspection data!")
    print("\nNext step: Run data_sanitizer.py to anonymise client names")
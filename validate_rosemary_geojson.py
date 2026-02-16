#!/usr/bin/env python3
"""
Validate and clean Rosemary's GeoJSON output
"""

import json
import sys

def validate_and_clean(input_file, output_file):
    """Validate Rosemary's GeoJSON and clean up for dashboard"""
    
    print("="*60)
    print("FAMM GeoJSON Validation & Cleanup")
    print("="*60)
    
    # Load file
    with open(input_file, 'r') as f:
        data = json.load(f)
    
    print(f"✅ Loaded {len(data['features'])} features")
    
    # Statistics
    high_conf = 0
    medium_conf = 0
    low_conf = 0
    
    cleaned_features = []
    
    for i, feature in enumerate(data['features']):
        props = feature['properties']
        
        # Clean up region name (remove "Western/Ashanti Region" → use correct region)
        region = props.get('region', '').replace('Western/Ashanti Region', '')
        
        # District-based region mapping for accuracy
        district = props.get('district', '')
        
        # Comprehensive region mapping based on known districts
        if any(d in district for d in ['Kwabre', 'Obuasi', 'Amansie', 'Adansi']):
            region = 'Ashanti Region'
        elif any(d in district for d in ['Tarkwa', 'Prestea', 'Wassa', 'Bibiani']):
            region = 'Western Region'
        elif any(d in district for d in ['Mfantseman', 'Komenda', 'Edina', 'Eguafo', 'Abirem', 'Cape Coast', 'Abura']):
            region = 'Central Region'
        elif any(d in district for d in ['Atiwa', 'Birim', 'Denkyembour', 'West Akim', 'East Akim']):
            region = 'Eastern Region'
        else:
            # If no match, keep original or default
            region = region.strip() if region.strip() else 'Unknown Region'
        
        # Update properties
        cleaned_props = {
            'confidence': props['confidence'],
            'district': district,
            'region': region,
            'date': props['date'],
            'area_ha': props['area_ha'],
            'tile_id': props.get('tile_id', ''),
            'alert_level': props['alert_level']
        }
        
        # Count by confidence
        conf = props['confidence']
        if conf >= 0.85:
            high_conf += 1
        elif conf >= 0.70:
            medium_conf += 1
        else:
            low_conf += 1
        
        cleaned_features.append({
            'type': 'Feature',
            'geometry': feature['geometry'],
            'properties': cleaned_props
        })
    
    # Create output
    output_data = {
        'type': 'FeatureCollection',
        'features': cleaned_features
    }
    
    # Save
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    # Print summary
    print("\n" + "="*60)
    print("VALIDATION RESULTS")
    print("="*60)
    print(f"✅ Total features: {len(cleaned_features)}")
    print(f"\nAlert Levels:")
    print(f"  - HIGH (≥85%):   {high_conf}")
    print(f"  - MEDIUM (70-85%): {medium_conf}")
    print(f"  - LOW (<70%):    {low_conf}")
    print(f"\n✅ Cleaned GeoJSON saved to: {output_file}")
    print("="*60)
    
    return True

if __name__ == '__main__':
    input_file = sys.argv[1] if len(sys.argv) > 1 else 'asm_monitoring_results.geojson'
    output_file = sys.argv[2] if len(sys.argv) > 2 else 'data/geojson/latest_detections.geojson'
    
    validate_and_clean(input_file, output_file)
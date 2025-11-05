#!/usr/bin/env python3
"""
IT Specialist Classification

Reads parquet files with job data and adds classification for IT specialist roles.
Classification: job title contains "IT" + "specialist" or "information technology" + "specialist" in any order.
"""

import pandas as pd
import glob
import os

def is_it_specialist(title):
    """Check if job title indicates IT specialist role"""
    if not title:
        return False
    
    title_lower = title.lower()
    
    # Check for ITSPEC pattern (common abbreviation)
    if "itspec" in title_lower:
        return True
    
    # Check for IT + SPEC patterns (with or without spaces)
    if "it" in title_lower and "spec" in title_lower:
        return True
    
    # Check for "IT" + "specialist" in any order
    has_it = "it " in title_lower or title_lower.startswith("it") or title_lower.endswith("it") or " it " in title_lower
    has_specialist = "specialist" in title_lower
    
    # Check for "information technology" + "specialist" in any order  
    has_info_tech = "information technology" in title_lower
    
    return (has_it and has_specialist) or (has_info_tech and has_specialist)

def classify_jobs_in_file(file_path):
    """Add IT specialist classification to a parquet file"""
    print(f"📊 Processing {file_path}...")
    
    df = pd.read_parquet(file_path)
    
    # Find the position title column (may have different names after flattening)
    title_cols = [col for col in df.columns if 'title' in col.lower() and 'position' in col.lower()]
    if not title_cols:
        title_cols = [col for col in df.columns if 'title' in col.lower()]
    
    if not title_cols:
        print(f"   ⚠️ No title column found in {file_path}")
        return
    
    title_col = title_cols[0]
    print(f"   Using title column: {title_col}")
    
    # Add classification
    df['is_it_specialist'] = df[title_col].apply(is_it_specialist)
    
    # Stats
    total_jobs = len(df)
    it_specialist_jobs = df['is_it_specialist'].sum()
    
    print(f"   ✅ {it_specialist_jobs}/{total_jobs} jobs classified as IT specialist ({it_specialist_jobs/total_jobs*100:.1f}%)")
    
    # Save back to parquet
    df.to_parquet(file_path, index=False)

def main():
    """Process all parquet files in data directory"""
    parquet_files = glob.glob("data/*.parquet")
    
    if not parquet_files:
        print("❌ No parquet files found in data/ directory")
        return
    
    print(f"🔍 Found {len(parquet_files)} parquet files")
    
    for file_path in parquet_files:
        try:
            classify_jobs_in_file(file_path)
        except Exception as e:
            print(f"❌ Error processing {file_path}: {e}")
    
    print("🎉 Classification complete!")

if __name__ == "__main__":
    main()
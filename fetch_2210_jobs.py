#!/usr/bin/env python3
"""
Simple USAJobs 2210 Job Fetcher

Fetches current 2210 (IT Specialist) jobs from USAJobs API and saves to parquet.
No crosswalks, no data rationalization - just flatten the JSON and store.
"""

import os
import json
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_api_headers():
    """Get API headers with authorization"""
    api_key = os.getenv("USAJOBS_API_TOKEN")
    
    if not api_key:
        raise ValueError("API key required. Set USAJOBS_API_TOKEN environment variable")
    
    return {
        "Host": "data.usajobs.gov",
        "Authorization-Key": api_key
    }

def flatten_json(obj, prefix='', sep='_'):
    """Recursively flatten nested JSON objects"""
    flattened = {}
    
    if isinstance(obj, dict):
        for key, value in obj.items():
            new_key = f"{prefix}{sep}{key}" if prefix else key
            
            if isinstance(value, (dict, list)):
                if isinstance(value, list) and value and not isinstance(value[0], (dict, list)):
                    # Simple list of primitives - join as string
                    flattened[new_key] = ', '.join(str(v) for v in value)
                elif isinstance(value, list) and value:
                    # List of objects - convert to JSON string
                    flattened[new_key] = json.dumps(value)
                elif isinstance(value, dict):
                    # Nested dict - recursively flatten
                    flattened.update(flatten_json(value, new_key, sep))
                else:
                    # Empty list or dict
                    flattened[new_key] = json.dumps(value) if value else None
            else:
                flattened[new_key] = value
    elif isinstance(obj, list):
        # Top-level list - convert to JSON string
        flattened[prefix] = json.dumps(obj)
    else:
        flattened[prefix] = obj
    
    return flattened

def filter_jobs_since_october(jobs):
    """Filter jobs to only include those posted since October 1, 2025"""
    from datetime import datetime
    cutoff_date = datetime(2025, 10, 1)
    
    filtered_jobs = []
    for job in jobs:
        # Extract date posted
        date_posted = None
        try:
            matched_obj = job.get("MatchedObjectDescriptor", {})
            date_str = matched_obj.get("PositionStartDate") or matched_obj.get("PublicationStartDate")
            
            if date_str:
                # Handle various date formats
                if 'T' in date_str:
                    date_str = date_str.split('T')[0]
                date_posted = datetime.strptime(date_str, '%Y-%m-%d')
        except (ValueError, AttributeError):
            continue
            
        if date_posted and date_posted >= cutoff_date:
            filtered_jobs.append(job)
    
    return filtered_jobs

def fetch_2210_jobs():
    """Fetch all current 2210 jobs posted since October 1, 2024"""
    print("🔍 Fetching 2210 jobs from USAJobs API...")
    
    headers = get_api_headers()
    base_url = "https://data.usajobs.gov/api/Search"
    
    params = {
        "JobCategoryCode": "2210",  # IT Specialist
        "ResultsPerPage": 500,
        "Fields": "full"
    }
    
    all_jobs = []
    page = 1
    
    while True:
        params["Page"] = page
        
        try:
            response = requests.get(base_url, headers=headers, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            search_result = data.get("SearchResult", {})
            items = search_result.get("SearchResultItems", [])
            
            if not items:
                break
                
            all_jobs.extend(items)
            print(f"   Page {page}: {len(items)} jobs")
            
            # Check if this is the last page
            total_count = search_result.get("SearchResultCountAll", 0)
            if len(all_jobs) >= total_count:
                break
                
            page += 1
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching page {page}: {e}")
            break
    
    print(f"✅ Fetched {len(all_jobs)} total 2210 jobs")
    
    # Filter to jobs since October 1, 2024
    filtered_jobs = filter_jobs_since_october(all_jobs)
    print(f"📅 Filtered to {len(filtered_jobs)} jobs posted since October 1, 2024")
    
    return filtered_jobs

def generate_metrics_json(df, output_dir="data"):
    """Generate metrics JSON file with key statistics"""
    from classify_it_specialist import is_it_specialist
    
    # Add IT specialist classification if not already present
    if 'is_it_specialist' not in df.columns:
        title_col = 'MatchedObjectDescriptor_PositionTitle'
        if title_col in df.columns:
            df['is_it_specialist'] = df[title_col].apply(is_it_specialist)
    
    # Calculate metrics
    total_jobs = len(df)
    it_specialist_jobs = df['is_it_specialist'].sum() if 'is_it_specialist' in df.columns else 0
    it_specialist_percentage = round((it_specialist_jobs / total_jobs * 100)) if total_jobs > 0 else 0
    
    # Find most recent IT specialist job
    most_recent_it_job = None
    most_recent_date = None
    most_recent_link = None
    
    if 'is_it_specialist' in df.columns and it_specialist_jobs > 0:
        it_jobs = df[df['is_it_specialist'] == True].copy()
        
        # Find date column
        date_cols = ['MatchedObjectDescriptor_PositionStartDate', 'MatchedObjectDescriptor_PublicationStartDate']
        date_col = None
        for col in date_cols:
            if col in it_jobs.columns:
                date_col = col
                break
        
        if date_col:
            # Convert to datetime and find most recent
            it_jobs[date_col] = pd.to_datetime(it_jobs[date_col], errors='coerce')
            most_recent_idx = it_jobs[date_col].idxmax()
            
            if not pd.isna(most_recent_idx):
                most_recent_row = it_jobs.loc[most_recent_idx]
                most_recent_date = most_recent_row[date_col].isoformat() if not pd.isna(most_recent_row[date_col]) else None
                most_recent_it_job = most_recent_row.get('MatchedObjectDescriptor_PositionTitle', 'Unknown')
                
                # Build USAJobs link
                position_uri = most_recent_row.get('MatchedObjectDescriptor_PositionURI')
                if position_uri:
                    # Clean up the link - remove port number if present
                    most_recent_link = position_uri.replace(':443', '')
    
    # Create metrics object
    metrics = {
        'generated_at': datetime.now().isoformat(),
        'total_2210_jobs': total_jobs,
        'it_specialist_jobs': int(it_specialist_jobs),
        'it_specialist_percentage': round(it_specialist_percentage, 1),
        'most_recent_it_specialist': {
            'title': most_recent_it_job,
            'date_posted': most_recent_date,
            'link': most_recent_link
        },
        'data_filter': 'Jobs posted since October 1, 2025',
        'other_bad_titles': [
            'IT PROGRAM MANAGER',
            'IT PROJECT MANAGER', 
            'Computer Systems Administrator (CSA)',
            'Information Technology Manager',
            'IT System Administrator'
        ]
    }
    
    # Save JSON
    metrics_file = f"{output_dir}/2210_metrics.json"
    with open(metrics_file, 'w') as f:
        json.dump(metrics, f, indent=2)
    
    print(f"📊 Metrics saved to {metrics_file}")
    return metrics

def main():
    """Main function"""
    try:
        # Fetch jobs
        jobs = fetch_2210_jobs()
        
        if not jobs:
            print("❌ No jobs fetched")
            return
        
        # Flatten each job
        print("📝 Flattening job data...")
        flattened_jobs = []
        
        for job in jobs:
            flattened = flatten_json(job)
            flattened['fetched_at'] = datetime.now().isoformat()
            flattened_jobs.append(flattened)
        
        # Convert to DataFrame
        df = pd.DataFrame(flattened_jobs)
        
        # Create data directory
        os.makedirs("data", exist_ok=True)
        
        # Save to parquet
        output_file = f"data/2210_jobs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.to_parquet(output_file, index=False)
        
        print(f"💾 Saved {len(df)} jobs to {output_file}")
        print(f"📊 Columns: {len(df.columns)}")
        
        # Generate metrics JSON
        metrics = generate_metrics_json(df)
        print(f"📈 Generated metrics: {metrics['it_specialist_jobs']}/{metrics['total_2210_jobs']} ({metrics['it_specialist_percentage']}%) IT specialist jobs")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    main()
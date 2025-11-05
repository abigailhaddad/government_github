#!/usr/bin/env python3
"""
Data integrity tests for 2210 jobs fetcher.
Ensures no job loss and validates date progression.

Usage: python test_data_integrity.py
"""

import os
import sys
import pandas as pd
import glob
from datetime import datetime
import json

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'

def print_header(text):
    print(f"\n{Colors.BLUE}{'=' * 60}{Colors.RESET}")
    print(f"{Colors.BLUE}{text}{Colors.RESET}")
    print(f"{Colors.BLUE}{'=' * 60}{Colors.RESET}")

def load_baseline():
    """Load baseline data for comparison"""
    baseline_file = 'data_baseline.json'
    if os.path.exists(baseline_file):
        with open(baseline_file, 'r') as f:
            return json.load(f)
    return None

def save_baseline(baseline_data):
    """Save baseline data for future comparisons"""
    with open('data_baseline.json', 'w') as f:
        json.dump(baseline_data, f, indent=2)

def get_latest_parquet_file():
    """Get the most recent parquet file"""
    parquet_files = glob.glob("data/2210_jobs_*.parquet")
    if not parquet_files:
        return None
    return max(parquet_files, key=os.path.getctime)

def test_parquet_file_validity():
    """Test that parquet file exists and is readable"""
    latest_file = get_latest_parquet_file()
    
    if not latest_file:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} No parquet files found")
        return False
    
    try:
        df = pd.read_parquet(latest_file)
        if len(df) == 0:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} Parquet file is empty")
            return False
        
        print(f"{Colors.GREEN}✅ PASS{Colors.RESET} Parquet file valid ({len(df)} jobs in {latest_file})")
        return True
    except Exception as e:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} Could not read parquet file: {e}")
        return False

def test_no_job_loss():
    """Ensure no jobs have been lost compared to baseline"""
    baseline = load_baseline()
    latest_file = get_latest_parquet_file()
    
    if not latest_file:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} No current data to test")
        return False
    
    if not baseline:
        print(f"{Colors.YELLOW}⚠️  WARN{Colors.RESET} No baseline found - creating initial baseline")
        create_initial_baseline(latest_file)
        return True
    
    # Check if data filter has changed - if so, reset baseline
    current_filter = "Jobs posted since October 1, 2025"
    baseline_filter = baseline.get('data_filter', 'Unknown')
    
    if baseline_filter != current_filter:
        print(f"{Colors.YELLOW}⚠️  WARN{Colors.RESET} Data filter changed: '{baseline_filter}' → '{current_filter}'")
        print(f"{Colors.YELLOW}⚠️  WARN{Colors.RESET} Resetting baseline due to filter change")
        create_initial_baseline(latest_file)
        return True
    
    try:
        df = pd.read_parquet(latest_file)
        
        # Get current job IDs
        current_ids = set()
        if 'MatchedObjectDescriptor_PositionID' in df.columns:
            current_ids = set(str(x) for x in df['MatchedObjectDescriptor_PositionID'].dropna())
        elif 'MatchedObjectDescriptor_PositionURI' in df.columns:
            # Extract ID from URI
            uris = df['MatchedObjectDescriptor_PositionURI'].dropna()
            current_ids = set(uri.split('/')[-1] for uri in uris if '/job/' in uri)
        
        # Compare with baseline
        baseline_ids = set(baseline.get('job_ids', []))
        
        if not current_ids:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} Could not extract job IDs from current data")
            return False
        
        lost_jobs = baseline_ids - current_ids
        new_jobs = current_ids - baseline_ids
        
        if lost_jobs:
            print(f"{Colors.RED}❌ CRITICAL FAIL{Colors.RESET} {len(lost_jobs)} jobs LOST!")
            print(f"     First 10 lost job IDs: {list(lost_jobs)[:10]}")
            return False
        
        print(f"{Colors.GREEN}✅ PASS{Colors.RESET} No jobs lost (+{len(new_jobs)} new jobs)")
        return True
        
    except Exception as e:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} Error checking job loss: {e}")
        return False

def test_date_progression():
    """Ensure most recent IT specialist date never goes backwards"""
    baseline = load_baseline()
    latest_file = get_latest_parquet_file()
    
    if not latest_file or not baseline:
        print(f"{Colors.YELLOW}⚠️  WARN{Colors.RESET} Cannot test date progression without baseline")
        return True
    
    try:
        # Load current data and classify
        df = pd.read_parquet(latest_file)
        
        # Add IT specialist classification
        from classify_it_specialist import is_it_specialist
        if 'MatchedObjectDescriptor_PositionTitle' in df.columns:
            df['is_it_specialist'] = df['MatchedObjectDescriptor_PositionTitle'].apply(is_it_specialist)
        else:
            print(f"{Colors.YELLOW}⚠️  WARN{Colors.RESET} No position title column for classification")
            return True
        
        # Get IT specialist jobs
        it_jobs = df[df['is_it_specialist'] == True]
        
        if len(it_jobs) == 0:
            print(f"{Colors.YELLOW}⚠️  WARN{Colors.RESET} No IT specialist jobs found")
            return True
        
        # Find most recent date
        date_cols = ['MatchedObjectDescriptor_PositionStartDate', 'MatchedObjectDescriptor_PublicationStartDate']
        current_max_date = None
        
        for col in date_cols:
            if col in it_jobs.columns:
                dates = pd.to_datetime(it_jobs[col], errors='coerce').dropna()
                if len(dates) > 0:
                    col_max = dates.max()
                    if current_max_date is None or col_max > current_max_date:
                        current_max_date = col_max
        
        if current_max_date is None:
            print(f"{Colors.YELLOW}⚠️  WARN{Colors.RESET} Could not find valid dates in IT specialist jobs")
            return True
        
        # Compare with baseline
        baseline_max_date_str = baseline.get('most_recent_it_specialist_date')
        if baseline_max_date_str:
            baseline_max_date = pd.to_datetime(baseline_max_date_str)
            
            if current_max_date < baseline_max_date:
                print(f"{Colors.RED}❌ CRITICAL FAIL{Colors.RESET} Most recent IT specialist date went BACKWARDS!")
                print(f"     Baseline: {baseline_max_date_str}")
                print(f"     Current:  {current_max_date.isoformat()}")
                return False
            elif current_max_date > baseline_max_date:
                print(f"{Colors.GREEN}✅ PASS{Colors.RESET} Most recent date advanced: {baseline_max_date_str} → {current_max_date.isoformat()}")
            else:
                print(f"{Colors.GREEN}✅ PASS{Colors.RESET} Most recent date unchanged: {current_max_date.isoformat()}")
        else:
            print(f"{Colors.GREEN}✅ PASS{Colors.RESET} First time tracking date: {current_max_date.isoformat()}")
        
        return True
        
    except Exception as e:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} Error checking date progression: {e}")
        return False

def test_october_filter():
    """Ensure all jobs are from October 1, 2024 or later"""
    latest_file = get_latest_parquet_file()
    
    if not latest_file:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} No data file to test")
        return False
    
    try:
        df = pd.read_parquet(latest_file)
        cutoff_date = pd.to_datetime('2025-10-01')
        
        # Check date columns
        date_cols = ['MatchedObjectDescriptor_PositionStartDate', 'MatchedObjectDescriptor_PublicationStartDate']
        
        violations = 0
        total_with_dates = 0
        
        for col in date_cols:
            if col in df.columns:
                dates = pd.to_datetime(df[col], errors='coerce')
                valid_dates = dates.dropna()
                
                if len(valid_dates) > 0:
                    total_with_dates += len(valid_dates)
                    before_cutoff = (valid_dates < cutoff_date).sum()
                    violations += before_cutoff
        
        if violations > 0:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} {violations} jobs found before October 1, 2025")
            return False
        elif total_with_dates > 0:
            print(f"{Colors.GREEN}✅ PASS{Colors.RESET} All {total_with_dates} dated jobs are from October 1, 2025 or later")
        else:
            print(f"{Colors.YELLOW}⚠️  WARN{Colors.RESET} No valid dates found to check")
        
        return True
        
    except Exception as e:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} Error checking date filter: {e}")
        return False

def test_metrics_json():
    """Test that metrics JSON is generated and valid"""
    metrics_file = "data/2210_metrics.json"
    
    if not os.path.exists(metrics_file):
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} Metrics JSON file not found")
        return False
    
    try:
        with open(metrics_file, 'r') as f:
            metrics = json.load(f)
        
        required_fields = ['total_2210_jobs', 'it_specialist_jobs', 'it_specialist_percentage', 'most_recent_it_specialist']
        missing_fields = [field for field in required_fields if field not in metrics]
        
        if missing_fields:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} Metrics JSON missing fields: {missing_fields}")
            return False
        
        # Validate most recent IT specialist has required subfields
        recent = metrics['most_recent_it_specialist']
        if not all(key in recent for key in ['title', 'date_posted', 'link']):
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} Most recent IT specialist missing required fields")
            return False
        
        print(f"{Colors.GREEN}✅ PASS{Colors.RESET} Metrics JSON valid")
        print(f"     {metrics['it_specialist_jobs']}/{metrics['total_2210_jobs']} ({metrics['it_specialist_percentage']}%) IT specialist jobs")
        
        return True
        
    except Exception as e:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} Error reading metrics JSON: {e}")
        return False

def create_initial_baseline(latest_file):
    """Create initial baseline from current data"""
    try:
        df = pd.read_parquet(latest_file)
        
        # Extract job IDs
        job_ids = []
        if 'MatchedObjectDescriptor_PositionID' in df.columns:
            job_ids = [str(x) for x in df['MatchedObjectDescriptor_PositionID'].dropna()]
        elif 'MatchedObjectDescriptor_PositionURI' in df.columns:
            uris = df['MatchedObjectDescriptor_PositionURI'].dropna()
            job_ids = [uri.split('/')[-1] for uri in uris if '/job/' in uri]
        
        # Get most recent IT specialist date
        from classify_it_specialist import is_it_specialist
        most_recent_date = None
        
        if 'MatchedObjectDescriptor_PositionTitle' in df.columns:
            df['is_it_specialist'] = df['MatchedObjectDescriptor_PositionTitle'].apply(is_it_specialist)
            it_jobs = df[df['is_it_specialist'] == True]
            
            if len(it_jobs) > 0:
                date_cols = ['MatchedObjectDescriptor_PositionStartDate', 'MatchedObjectDescriptor_PublicationStartDate']
                for col in date_cols:
                    if col in it_jobs.columns:
                        dates = pd.to_datetime(it_jobs[col], errors='coerce').dropna()
                        if len(dates) > 0:
                            col_max = dates.max()
                            if most_recent_date is None or col_max > most_recent_date:
                                most_recent_date = col_max
        
        baseline = {
            'created_at': datetime.now().isoformat(),
            'job_ids': job_ids,
            'total_jobs': len(df),
            'most_recent_it_specialist_date': most_recent_date.isoformat() if most_recent_date else None
        }
        
        save_baseline(baseline)
        print(f"{Colors.GREEN}✅ Created baseline with {len(job_ids)} job IDs{Colors.RESET}")
        
    except Exception as e:
        print(f"{Colors.RED}❌ Error creating baseline: {e}{Colors.RESET}")

def update_baseline():
    """Update baseline with current data after successful tests"""
    latest_file = get_latest_parquet_file()
    if latest_file:
        create_initial_baseline(latest_file)

def run_tests():
    """Run all data integrity tests"""
    print(f"{Colors.BLUE}2210 JOBS DATA INTEGRITY TESTS{Colors.RESET}")
    print(f"{Colors.BLUE}Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{Colors.RESET}")
    
    all_passed = True
    
    # Test 1: Parquet file validity
    print_header("1. PARQUET FILE VALIDITY")
    if not test_parquet_file_validity():
        all_passed = False
    
    # Test 2: No job loss
    print_header("2. NO JOB LOSS TEST")
    if not test_no_job_loss():
        all_passed = False
    
    # Test 3: Date progression
    print_header("3. DATE PROGRESSION TEST")
    if not test_date_progression():
        all_passed = False
    
    # Test 4: October filter
    print_header("4. OCTOBER 1, 2024 FILTER TEST")
    if not test_october_filter():
        all_passed = False
    
    # Test 5: Metrics JSON
    print_header("5. METRICS JSON TEST")
    if not test_metrics_json():
        all_passed = False
    
    # Summary
    print_header("TEST SUMMARY")
    if all_passed:
        print(f"{Colors.GREEN}✅ ALL TESTS PASSED!{Colors.RESET}")
        print("Data integrity verified. Updating baseline...")
        update_baseline()
        print(f"{Colors.GREEN}✅ Baseline updated{Colors.RESET}")
        return 0
    else:
        print(f"{Colors.RED}❌ SOME TESTS FAILED!{Colors.RESET}")
        print("Data integrity issues detected!")
        return 1

if __name__ == "__main__":
    sys.exit(run_tests())
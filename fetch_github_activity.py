#!/usr/bin/env python3
"""
Federal Agency GitHub Activity Fetcher

Fetches GitHub activity (commits, PRs, issues) for federal agencies and saves metrics.
"""

import os
import json
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Federal agency GitHub organizations
FEDERAL_AGENCIES = {
    # Original 15 agencies
    'United States Digital Service': 'usds',
    'General Services Administration': '18f',
    'National Aeronautics and Space Administration': 'nasa',
    'Department of Defense': 'deptofdefense',
    'Department of Veterans Affairs': 'department-of-veterans-affairs',
    'Department of Health and Human Services': 'hhs',
    'Department of Energy': 'DeptOfEnergy',
    'Department of Justice': 'usdoj',
    'Department of Homeland Security': 'dhs-gov',
    'National Institute of Standards and Technology': 'usnistgov',
    'United States Geological Survey': 'usgs',
    'National Oceanic and Atmospheric Administration': 'noaa-gsl',
    'Centers for Disease Control and Prevention': 'cdcepi',
    'National Institutes of Health': 'ncbi',
    'Department of the Treasury': 'US-Department-of-the-Treasury',
    
    # Additional agencies discovered from government.github.com
    'Environmental Protection Agency': 'usepa',                          # 661 repos
    'Consumer Financial Protection Bureau': 'cfpb',                      # 358 repos 
    'Federal Communications Commission': 'fcc',                          # 97 repos
    'National Park Service': 'nationalparkservice',                      # 138 repos
    'Department of State': 'usstatedept',                               # 85 repos
    'Department of Energy (Energy Apps)': 'energyapps',                 # 64 repos
    'Food and Drug Administration': 'fda',                              # 57 repos
    'Library of Congress': 'libraryofcongress',                         # 55 repos
    'White House': 'whitehouse',                                        # 41 repos
    'Peace Corps': 'peacecorps',                                        # 34 repos
    'Small Business Administration': 'USSBA',                           # 24 repos
    'Social Security Administration': 'SSAgov',                         # 21 repos
    'Federal Emergency Management Agency': 'fema',                       # 9 repos
    'United States Agency for International Development': 'usaid',       # 8 repos
    'Internal Revenue Service': 'IRSgov',                               # 5 repos
    'United States Department of Agriculture': 'usda',                  # 4 repos
    'U.S. Citizenship and Immigration Services': 'uscis'                # 2 repos
}

def get_github_headers():
    """Get GitHub API headers with authorization"""
    token = os.getenv("GITHUB_TOKEN")
    
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "Federal-Agency-Tracker"
    }
    
    if token:
        headers["Authorization"] = f"token {token}"
    
    return headers

def make_github_request(url, headers, params=None, max_retries=3):
    """Make a rate-limited GitHub API request with retry logic"""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            # Check rate limit headers
            remaining = int(response.headers.get('X-RateLimit-Remaining', 1))
            reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
            
            if response.status_code == 403 and remaining == 0:
                # Rate limit exceeded
                wait_time = max(reset_time - int(time.time()) + 60, 60)  # Add 1 min buffer
                print(f"   Rate limit exceeded. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            
            if response.status_code == 404:
                return response  # Return 404s as-is
            
            if response.status_code == 409:
                # Conflict - usually empty repo or access issue
                print(f"   ⚠️  Repo conflict (likely empty): {url}")
                return response  # Return 409s as-is, don't retry
            
            response.raise_for_status()
            
            # Add small delay between requests to be nice to API
            time.sleep(0.2)  # Reduced from 0.5s to 0.2s
            return response
            
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                print(f"   ❌ Request failed after {max_retries} attempts: {e}")
                raise e
            wait_time = (2 ** attempt) + 1  # 1, 3, 7 seconds
            print(f"   ⚠️  Request failed (attempt {attempt + 1}), retrying in {wait_time}s...")
            time.sleep(wait_time)
    
    return None

def get_org_activity(org_name, days_back=30):
    """Get activity for a single organization over the last N days"""
    headers = get_github_headers()
    since_date = datetime.now() - timedelta(days=days_back)
    since_iso = since_date.isoformat()
    
    activity = {
        'name': org_name,
        'daily_activity': {},
        'total_repos': 0,
        'total_commits': 0,
        'total_prs': 0,
        'total_issues': 0,
        'last_activity': None,
        'exists': True
    }
    
    # Initialize daily activity for last 90 days
    for i in range(days_back):
        date = datetime.now() - timedelta(days=i)
        date_str = date.strftime('%Y-%m-%d')
        activity['daily_activity'][date_str] = {
            'commits': 0,
            'prs': 0,
            'issues': 0,
            'total': 0
        }
    
    try:
        # Get organization info and repos
        org_url = f"https://api.github.com/orgs/{org_name}"
        org_response = make_github_request(org_url, headers)
        
        if org_response.status_code == 404:
            print(f"❌ Organization {org_name} not found")
            activity['exists'] = False
            return activity
        
        # Get actual repo count from organization data
        org_data = org_response.json()
        activity['total_repos'] = org_data.get('public_repos', 0)
        
        # Get repositories for analysis (we'll still only analyze a subset)
        repos_url = f"https://api.github.com/orgs/{org_name}/repos"
        repos_response = make_github_request(repos_url, headers, params={'per_page': 100, 'sort': 'updated'})
        
        repos = repos_response.json()
        
        print(f"   Found {len(repos)} repositories, checking for recent commits...")
        
        # Check repos in batches, keep going if we find activity
        batch_size = 10
        repos_checked = 0
        consecutive_inactive = 0
        max_consecutive_inactive = 5  # Stop if we hit 5 inactive repos in a row
        active_repos_count = 0  # Track repos with activity
        active_repos_details = []  # Track active repos with their activity counts
        
        print(f"   Checking repos in batches of {batch_size}, will continue while finding activity...")
        
        for repo in repos:
            repo_name = repo['name']
            repo_has_recent_activity = False
            repo_commits = 0
            repo_prs = 0
            
            # Get commits for this repo
            commits_url = f"https://api.github.com/repos/{org_name}/{repo_name}/commits"
            try:
                commits_response = make_github_request(
                    commits_url, 
                    headers, 
                    params={'since': since_iso, 'per_page': 100}
                )
                
                if commits_response and commits_response.status_code == 200:
                    commits = commits_response.json()
                    repo_commits = len(commits)
                    activity['total_commits'] += repo_commits
                    
                    if len(commits) > 0:
                        repo_has_recent_activity = True
                elif commits_response and commits_response.status_code == 409:
                    # Empty repo or conflict - just skip it
                    pass
                else:
                    # Some other error - skip this repo
                    pass
                
                # Count commits by day and store commit info (only if we have commits)
                if commits_response and commits_response.status_code == 200:
                    commits = commits_response.json()
                    for commit in commits:
                        commit_date = commit['commit']['author']['date'][:10]  # YYYY-MM-DD
                        if commit_date in activity['daily_activity']:
                            # Initialize commits list if not exists
                            if 'commit_links' not in activity['daily_activity'][commit_date]:
                                activity['daily_activity'][commit_date]['commit_links'] = []
                            
                            # Store commit info for tooltips
                            commit_info = {
                                'repo_name': repo_name,
                                'repo_url': f"https://github.com/{org_name}/{repo_name}",
                                'html_url': commit['html_url'],
                                'message': commit['commit']['message'].split('\n')[0][:80],  # First line, truncated
                                'author': commit['commit']['author']['name'],
                                'sha': commit['sha'][:7]
                            }
                            activity['daily_activity'][commit_date]['commit_links'].append(commit_info)
                            
                            activity['daily_activity'][commit_date]['commits'] += 1
                            activity['daily_activity'][commit_date]['total'] += 1
                            
                            if not activity['last_activity'] or commit_date > activity['last_activity']:
                                activity['last_activity'] = commit_date
                
            except requests.exceptions.RequestException:
                continue  # Skip this repo if API call fails
                
            # Get PRs for this repo
            prs_url = f"https://api.github.com/repos/{org_name}/{repo_name}/pulls"
            try:
                prs_response = make_github_request(
                    prs_url,
                    headers,
                    params={'state': 'all', 'per_page': 100}
                )
                
                if prs_response and prs_response.status_code == 200:
                    all_prs = prs_response.json()
                    
                    # Filter PRs by date (GitHub API since param doesn't work properly for PRs)
                    recent_prs = []
                    for pr in all_prs:
                        pr_date = datetime.strptime(pr['created_at'][:10], '%Y-%m-%d')
                        if pr_date >= since_date:
                            recent_prs.append(pr)
                    
                    repo_prs = len(recent_prs)
                    activity['total_prs'] += repo_prs
                    
                    if len(recent_prs) > 0:
                        repo_has_recent_activity = True
                    
                    # Count PRs by day and store PR info
                    for pr in recent_prs:
                        pr_date = pr['created_at'][:10]  # YYYY-MM-DD
                        if pr_date in activity['daily_activity']:
                            # Initialize PR links list if not exists
                            if 'pr_links' not in activity['daily_activity'][pr_date]:
                                activity['daily_activity'][pr_date]['pr_links'] = []
                            
                            # Store PR info for tooltips
                            pr_info = {
                                'repo_name': repo_name,
                                'repo_url': f"https://github.com/{org_name}/{repo_name}",
                                'html_url': pr['html_url'],
                                'title': pr['title'][:80],  # Truncated title
                                'author': pr['user']['login'],
                                'number': pr['number'],
                                'state': pr['state']
                            }
                            activity['daily_activity'][pr_date]['pr_links'].append(pr_info)
                            
                            activity['daily_activity'][pr_date]['prs'] += 1
                            activity['daily_activity'][pr_date]['total'] += 1
                            
                            if not activity['last_activity'] or pr_date > activity['last_activity']:
                                activity['last_activity'] = pr_date
                
            except requests.exceptions.RequestException:
                continue
            
            # Track this repo's activity regardless of whether it had recent activity
            # (so we can rank all repos by activity level)
            total_repo_activity = repo_commits + repo_prs
            if total_repo_activity > 0:
                active_repos_details.append({
                    'name': repo_name,
                    'commits': repo_commits,
                    'prs': repo_prs,
                    'total_activity': total_repo_activity,
                    'html_url': f"https://github.com/{org_name}/{repo_name}"
                })
            
            if repo_has_recent_activity:
                consecutive_inactive = 0  # Reset counter
                active_repos_count += 1
            else:
                consecutive_inactive += 1
            
            repos_checked += 1
            
            # Stop if we've hit too many consecutive inactive repos (unless we're in first batch)
            if consecutive_inactive >= max_consecutive_inactive and repos_checked > batch_size:
                print(f"   Stopping after {consecutive_inactive} consecutive inactive repos")
                break
                
            # Progress update every batch
            if repos_checked % batch_size == 0:
                active_count = activity['total_commits'] + activity['total_prs'] 
                print(f"   Checked {repos_checked} repos, found {active_count} total events so far...")
        
        
        # Sort active repos by total activity and keep top 10
        active_repos_details.sort(key=lambda x: x['total_activity'], reverse=True)
        activity['active_repos'] = active_repos_count
        activity['top_active_repos'] = active_repos_details[:10]  # Top 10 most active repos
        
        print(f"   Total activity: {activity['total_commits']} commits, {activity['total_prs']} PRs across {active_repos_count} active repos")
        if active_repos_details:
            print(f"   Top active repos: {', '.join([r['name'] for r in active_repos_details[:3]])}")
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching data for {org_name}: {e}")
        # Don't set exists=False for API errors - leave it undefined to trigger failure
        # Only set exists=False for confirmed 404s (handled above)
        return {}  # Return empty dict to signal complete failure
    
    return activity

def calculate_activity_level(activity_data):
    """Calculate activity level based on recent activity"""
    if not activity_data['exists']:
        return 'dead'
    
    total_activity = activity_data['total_commits'] + activity_data['total_prs']
    
    if total_activity >= 100:
        return 'high'
    elif total_activity >= 20:
        return 'medium'
    elif total_activity >= 5:
        return 'low'
    else:
        return 'dead'

def generate_activity_json(activities, output_dir="deploy/data"):
    """Generate activity JSON for the frontend"""
    
    # Calculate summary stats
    summary = {
        'generated_at': datetime.now().isoformat(),
        'agencies': {},
        'total_agencies': len(activities),
        'active_agencies': 0,
        'total_commits': 0,
        'total_prs': 0
    }
    
    for agency_name, org_name in FEDERAL_AGENCIES.items():
        activity = activities.get(org_name, {})
        
        if not activity or not activity.get('exists', False):
            summary['agencies'][agency_name] = {
                'org_name': org_name,
                'activity_level': 'dead',
                'repos': 0,
                'commits': 0,
                'prs': 0,
                'last_activity': None,
                'daily_activity': {},
                'exists': False
            }
            continue
        
        activity_level = calculate_activity_level(activity)
        
        # Calculate days since last activity
        last_activity_days = None
        if activity['last_activity']:
            last_date = datetime.strptime(activity['last_activity'], '%Y-%m-%d')
            last_activity_days = (datetime.now() - last_date).days
        
        summary['agencies'][agency_name] = {
            'org_name': org_name,
            'activity_level': activity_level,
            'repos': activity['total_repos'],
            'active_repos': activity.get('active_repos', 0),
            'top_active_repos': activity.get('top_active_repos', []),
            'commits': activity['total_commits'],
            'prs': activity['total_prs'],
            'last_activity': activity['last_activity'],
            'last_activity_days': last_activity_days,
            'daily_activity': activity['daily_activity'],
            'exists': True
        }
        
        if activity_level != 'dead':
            summary['active_agencies'] += 1
        
        summary['total_commits'] += activity['total_commits']
        summary['total_prs'] += activity['total_prs']
    
    # Save JSON
    os.makedirs(output_dir, exist_ok=True)
    activity_file = f"{output_dir}/github_activity.json"
    
    with open(activity_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"📊 Activity data saved to {activity_file}")
    return summary

def main():
    """Main function"""
    print("🔍 Fetching GitHub activity for federal agencies...")
    
    activities = {}
    failed_agencies = []
    
    for i, (agency_name, org_name) in enumerate(FEDERAL_AGENCIES.items(), 1):
        print(f"\n📊 [{i}/{len(FEDERAL_AGENCIES)}] Fetching activity for {agency_name} ({org_name})...")
        activity = get_org_activity(org_name)
        activities[org_name] = activity
        
        # Track agencies that failed to fetch (API errors, not just missing orgs)
        if not activity.get('exists') and 'exists' not in activity:
            failed_agencies.append(f"{agency_name} ({org_name})")
    
    # Exit with error if any agency completely failed to fetch
    if failed_agencies:
        print(f"\n❌ CRITICAL: Failed to fetch data for agencies: {', '.join(failed_agencies)}")
        print("This indicates API errors or connectivity issues.")
        exit(1)
    
    # Generate summary JSON
    summary = generate_activity_json(activities)
    
    active_count = summary['active_agencies']
    total_count = summary['total_agencies']
    existing_count = sum(1 for agency in summary['agencies'].values() if agency['exists'])
    
    print(f"\n✅ Complete! {existing_count}/{total_count} agencies accessible")
    print(f"📊 {active_count} agencies have recent activity")
    print(f"📈 Total: {summary['total_commits']} commits, {summary['total_prs']} PRs")
    
    # Verify we got data for expected number of agencies
    if existing_count < len(FEDERAL_AGENCIES) * 0.8:  # Allow for some agencies to be missing/private
        print(f"\n⚠️  WARNING: Only {existing_count}/{total_count} agencies accessible - check for org name changes")
    
    return summary

if __name__ == "__main__":
    main()
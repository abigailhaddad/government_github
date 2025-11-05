# USAJobs IT Specialist Tracker

A real-time tracker monitoring compliance with OPM's plain language job title directive.

## 🎯 What it tracks

Days since the last "IT Specialist" posting on USAJobs, plus the percentage of GS-2210 jobs still using this generic title that OPM explicitly said not to use.

## 🚀 Live Site

The tracker is deployed at: [your-netlify-url-here]

## 📊 How it works

1. **Daily data collection** - GitHub Action fetches current 2210 jobs from USAJobs API
2. **Smart classification** - Identifies "IT Specialist" jobs using pattern matching
3. **Data integrity tests** - Ensures no job loss and validates date progression  
4. **Real-time display** - Website shows days since last posting and compliance percentage

## 🛠 Setup

1. Set `USAJOBS_API_TOKEN` secret in GitHub repo settings
2. GitHub Action runs daily at 6 AM UTC
3. Netlify auto-deploys from `main` branch

## 📁 Repository Structure

**Root (GitHub Actions):**
- `fetch_2210_jobs.py` - Fetches and processes USAJobs data
- `classify_it_specialist.py` - Classifies job titles  
- `test_data_integrity.py` - Validates data quality
- `.github/workflows/` - Daily automation

**Deploy folder (Netlify):**
- `deploy/index.html` - Main tracker webpage
- `deploy/data/` - Generated JSON data
- `deploy/netlify.toml` - Deployment config

## 📈 Data

- Filters to jobs posted since October 1, 2025
- Generates `deploy/data/2210_metrics.json` with current stats
- Includes examples of other non-compliant titles
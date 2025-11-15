# Federal Agency GitHub Activity Tracker

A visualization tool that tracks public GitHub activity for 32 federal agencies, monitoring commits and pull requests over the last 30 days.

## 🎯 What it tracks

- **Commits to default branches** across federal agency repositories
- **Pull request creation** activity 
- **Active repositories** with recent development
- **Agency-level activity heatmaps** showing daily development patterns

## 🚀 Live Site

[View the Federal GitHub Activity Tracker](https://federal-github-activity.netlify.app)

## 📊 How it works

1. **Daily data collection** - GitHub Action fetches activity from 32 federal agency organizations
2. **Activity aggregation** - Processes commits, PRs, and repository metrics
3. **Data validation** - Ensures data quality and structure integrity
4. **Interactive visualization** - GitHub-style heatmap with sortable metrics

## 🛠 Technology Stack

- **Frontend**: Vanilla HTML/CSS/JavaScript with responsive design
- **Backend**: Python script using GitHub API v4
- **Deployment**: Netlify static site hosting
- **Automation**: GitHub Actions for daily updates
- **Data Validation**: Automated quality checks before deployment

## 📁 Repository Structure

**Root (GitHub Actions):**
- `fetch_github_activity.py` - Fetches and processes GitHub data
- `.github/workflows/` - Daily automation and validation
- `requirements.txt` - Python dependencies

**Deploy folder (Netlify):**
- `deploy/index.html` - Main tracker webpage
- `deploy/data/github_activity.json` - Generated activity data
- `deploy/netlify.toml` - Deployment configuration

## 📈 Data Coverage

Monitors 32 federal agencies including USDS, 18F, NASA, Department of Defense, Department of Veterans Affairs, HHS, DOE, and more.

**Note**: Data shows only **public** repositories and measures creation events (commits to default branches, PR creation), not merges or completions. Agencies may have additional private development activity.

## 🔗 Related Policy

This tracker supports transparency around the [Federal Source Code Policy](https://digital.gov/resources/requirements-for-achieving-efficiency-transparency-and-innovation-through-reusable-and-open-source-software/) pilot program requiring agencies to release at least 20% of new custom-developed code as open source software.

---

Created by [Abigail Haddad](https://abigailhaddad.netlify.app/) | [Blog](https://presentofcoding.substack.com/)
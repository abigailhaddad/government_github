# Hosting and deploy

The site is the `deploy/` folder. The daily workflow (`.github/workflows/fetch-github-data.yml`) fetches new data into `deploy/data/github_activity.json`, validates it, and publishes the folder to Cloudflare Pages. Cloudflare Pages is the only host.

- Address: https://federal-github-activity.abigailhaddad.com
- Pages project: `federal-github-activity` (also reachable at `federal-github-activity.pages.dev`)
- Schedule: 02:00 UTC every day (GitHub often starts it later), and on demand from the Actions tab.

## Secret and variables

Set these in the repo under Settings, Secrets and variables, Actions.

| Name | Kind | What it is |
|---|---|---|
| `CLOUDFLARE_API_TOKEN` | secret | A Cloudflare token with Account, Cloudflare Pages, Edit. GitHub never shows a stored value, so make a new token or roll the old one. |
| `CLOUDFLARE_ACCOUNT_ID` | variable | The Cloudflare account id. |
| `CLOUDFLARE_PROJECT` | variable, optional | The Pages project name. Defaults to `federal-github-activity`. |
| `GH_TOKEN` | secret | A GitHub token the data fetch uses to read public repositories. |

A step before the deploy fails the run when the token, the account id, or any of `deploy/index.html`, `deploy/_headers` or `deploy/data/github_activity.json` is missing, so a run can't finish green without publishing.

## Moving it to a new owner

1. Create a Pages project in your Cloudflare account (Workers & Pages, Create, Pages, Upload assets). Production branch `main`. Use the default name, or set `CLOUDFLARE_PROJECT`.
2. Add the secret and variables above.
3. Run the workflow by hand (Actions, Fetch Federal GitHub Activity, Run workflow) and check the `pages.dev` address loads with today's `generated_at`.
4. For a custom domain, add it under the project's Custom domains. If the domain's DNS is on Cloudflare, point a CNAME for the name at `<project>.pages.dev`.

The footer link in `deploy/index.html` points at `abigailhaddad.com`; change it for a new owner.

## Cache headers

`deploy/_headers` sets a five-minute cache on `/data/*` so a refreshed data file shows up quickly.

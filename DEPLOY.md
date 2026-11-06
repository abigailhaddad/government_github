# Moving the site to Cloudflare Pages

The site is the `deploy/` folder. The daily workflow (`.github/workflows/fetch-github-data.yml`) refreshes `deploy/data/github_activity.json` and publishes the folder.

## Where it is now

- Published to GitHub Pages by the workflow (`actions/deploy-pages`).
- Custom domain `federal-github-activity.abigailhaddad.com` is a DNS CNAME to `abigailhaddad.github.io`. Over `http` it works. Over `https` the certificate doesn't match. "Enforce HTTPS" is off in the Pages settings.
- `abigailhaddad.com` DNS is on Cloudflare, like the other sites.

## What this PR changes

- Adds a "Deploy to Cloudflare Pages" step after the GitHub Pages deploy. It is skipped until the repo variable `CLOUDFLARE_ACCOUNT_ID` exists, so merging changes nothing by itself.
- Adds `deploy/_headers` (the 5-minute cache on `/data/*`, replacing `deploy/netlify.toml`).
- Deletes `deploy/netlify.toml` and `deploy/_redirects`. They were Netlify-only. The workflow stopped deploying to Netlify earlier.
- Footer "Website" link now points at `abigailhaddad.com`, and the README no longer says Netlify.

GitHub Pages keeps working until you remove it in step 8.

## Steps, in this order

1. **Merge the PR.** Nothing changes yet.
2. **Create the Pages project.** Cloudflare dashboard, Workers & Pages, Create, Pages, Upload assets. Name it `federal-github-activity` (the workflow's default). Production branch `main`. Any placeholder file will do for the first upload. A different name needs the repo variable `CLOUDFLARE_PROJECT` set to it.
3. **Add to this repo** (Settings, Secrets and variables, Actions):
   - Secret `CLOUDFLARE_API_TOKEN`: a token with Account, Cloudflare Pages, Edit. The `ota-watch` repo uses one. GitHub won't show its value, so make a new token or paste the one you saved.
   - Variable `CLOUDFLARE_ACCOUNT_ID`: your Cloudflare account id. The `dod` repo has the same variable.
4. **Run the workflow by hand** (Actions, Fetch Federal GitHub Activity, Run workflow). Check the "Deploy to Cloudflare Pages" step is green and `https://federal-github-activity.pages.dev` loads with current data. (If you named the project differently, the address changes.)
5. **Switch the domain.** In Cloudflare DNS for `abigailhaddad.com`, delete the CNAME `federal-github-activity` that points at `abigailhaddad.github.io`. Then in the Pages project, Custom domains, add `federal-github-activity.abigailhaddad.com`. Cloudflare creates the new record and issues the certificate, usually within minutes. The site can be down for those minutes. Deleting the record first is needed or Cloudflare refuses the domain.
6. **Check** `https://federal-github-activity.abigailhaddad.com` loads over HTTPS, the heatmap renders, and the data's `generated_at` is today's.
7. **Wait one scheduled run** (the cron is 02:00 UTC, though GitHub has been running it around 08:00 UTC) and confirm the Cloudflare step passes unattended.
8. **Retire GitHub Pages**, in a follow-up commit:
   - In the workflow, delete the "Upload the site" and "Deploy to GitHub Pages" steps, the `environment:` block, and the `pages: write` and `id-token: write` permissions. Remove the `if:` line from the Cloudflare step.
   - Delete `deploy/CNAME`.
   - In GitHub, Settings, Pages: clear the custom domain, and set the source to none.

## Undo

Before step 5: nothing to undo. After it: re-create the CNAME `federal-github-activity` to `abigailhaddad.github.io` and remove the custom domain from the Pages project. GitHub Pages is still deploying until step 8.

## Not checked

- Why the certificate on the GitHub Pages domain is wrong. I did not need it for the move.
- The Cloudflare token and project. I have no access to Cloudflare, so the new step has not run.
- `federal-github-activity.netlify.app` still answers with a redirect. It is a separate Netlify site. Delete it in Netlify if you don't want it.

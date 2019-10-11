This website was created with [Docusaurus](https://docusaurus.io/).

# Publishing docs

## Required Installations:
1. Make sure [Node.js is installed](https://nodejs.org/en/download/).
2. Make sure yarn is installed (`brew install yarn`).
3. Install docusaurus (from the `website` directory, `npm install docusaurus`).

For steps 1 and 2, see details [here](https://docusaurus.io/docs/en/installation).

## Testing your docs updates on localhost:
1. Start the webserver by doing `yarn start`, it will redirect to a published docs page on your default browser.

## Publishing to public docs site:
1. Make sure that your local master has the intended docs changes to be published.
2. Run the following script from inside /website directory,
    ```
    GIT_USER=<GIT_USER> \ # The publisher's git username with push access to the waltz repository.
    CURRENT_BRANCH=master \ # (Optional) `master` by default. Can publish a specific branch by updating here.
    USE_SSH=true \ # (Optional) `false` by default. If false, uses HTTPS.
    yarn run publish-gh-pages
    ```
3. It updates the `gh-pages` branch on Waltz remote repo, or creates it if there is none already.
4. The website is served from the `gh-pages` branch.
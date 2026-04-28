# Entmoot Website

This is the Docusaurus documentation site for Entmoot.

## Local Development

```sh
npm install
npm run start
```

## Build

```sh
npm run typecheck
npm run build
```

`npm run build` copies `paper/main.pdf` and `paper/evolution.pdf` into
`static/papers/` before running the Docusaurus production build.

## Deployment

GitHub Pages deployment is handled by `.github/workflows/docs.yml` on pushes
to `main`.

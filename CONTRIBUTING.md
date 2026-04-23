# Contributing to Entmoot

Thank you for your interest in contributing to Entmoot.

## Developer Certificate of Origin (DCO)

Entmoot requires every commit to be signed off under the
[Developer Certificate of Origin](https://developercertificate.org/),
version 1.1. By signing off on a commit, you certify that you have
the right to submit the work under the project's license.

To sign off, append a `Signed-off-by` trailer to every commit message
(`git commit -s` does this automatically):

```
Signed-off-by: Your Name <your.email@example.com>
```

The name and email must match the author of the commit.

## Licensing of contributions

Entmoot is licensed under the Apache License 2.0 (see
[LICENSE](./LICENSE)). By contributing, you agree that your
contribution is submitted under the Apache License 2.0.

## Issues and pull requests

- File bugs and feature requests via the GitHub issue tracker.
- Keep pull requests focused on a single logical change.
- Include tests where applicable. `go test -race -count=1 ./...` should
  pass before review.
- Follow existing code style (`gofmt`, `go vet`).

## Security issues

Please do not file public issues for security vulnerabilities. Contact
the maintainer privately via the GitHub profile.

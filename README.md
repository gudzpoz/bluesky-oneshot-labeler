# bluesky-oneshot-labeler

Labeling services like [@moderation.bsky.app](https://bsky.app/profile/moderation.bsky.app)
usually label contents on a per-post basis.
However, for those who never label their NSFW/sensitive contents,
it might be best to mark their whole account as not-suitable.

This labeler lets you specify a upstream labeling services,
and marks the posters of NSFW/sensitive contents as not-suitable.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

## MakeFile

Run build make command with tests
```bash
make all
```

Build the application
```bash
make build
```

Run the application
```bash
make run
```

Live reload the application:
```bash
make watch
```

Run the test suite:
```bash
make test
```

Clean up binary from the last build:
```bash
make clean
```

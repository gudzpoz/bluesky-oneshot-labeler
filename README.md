# bluesky-oneshot-labeler

Labeling services like [@moderation.bsky.app](https://bsky.app/profile/moderation.bsky.app)
usually label contents on a per-post basis.
However, for those who never label their NSFW/sensitive contents,
it might be best to mark their whole account as not-suitable.

This labeler lets you specify a upstream labeling services,
and marks the posters of NSFW/sensitive contents as not-suitable.

Also, this labeler provides a feed that blocks NSFW/sensitive contents
using the extracted user labels. Have a look at [@oneshot.iroiro.party]
for an example.

[@oneshot.iroiro.party]: https://bsky.app/profile/oneshot.iroiro.party/feed/oneshot

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### MakeFile

Build the application
```bash
make build
```

Run the application
```bash
go run cmd/api/main.go
```

### Config

Please have a look at [`.env.example`](./.env.example) for the configuration.
Copy it to `.env` and edit it according to your environment.

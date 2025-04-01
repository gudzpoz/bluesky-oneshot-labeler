# bluesky-oneshot-feed

```text
   +----------------------------------------------+    +----------------------------------------+
   | Upstream labeler (e.g. @moderation.bsky.app) |    | Moderation reports (from an allowlist) |
   +---------------------+------------------------+    +-------------------+--------------------+
                         |                                 -------/        | MODERATOR_HANDLES
                         | Post-wise labels       --------/ (Bluesky UI)   |
                        \|/               -------/                         |
              +----------+----------+----/  Writes to         +------------+-------------+
              |  This no-op labeler |------------------------>| External CSV file (DIDs) |
              +----------+----------+                         +------------+-------------+
                         |                                                 | EXTERNAL_BLOCK_LIST
                         |                                                 |
                        \|/                                               \|/
             +-----------+----------+                     +----------------+----------------+
             |    User block list   |--------+------------+ Manually maintained block list  |
             +----------------------+        |            +---------------------------------+
                                             |
                                             |            +--------------------+
                                             +------------+ Customized filters | feed_filter_user.go
                                             |            +--------------------+
                                             |            +------------------------+
                                             +------------+ vit-base-nsfw-detector | nsfw-vit/
                                             |            +------------------------+
                            +----------------+----------------+
                            |                                 |
                            |       Hopefully SFW Feed?       |
                            +---------------------------------+
```

Labeling services like [@moderation.bsky.app](https://bsky.app/profile/moderation.bsky.app)
usually label contents on a per-post basis.
However, for those who never label their NSFW/sensitive contents,
it might be best to mark their whole account as not-suitable.

This labeler lets you specify a upstream labeling services,
and marks the posters of NSFW/sensitive contents as not-suitable.

## This labeler is currently no-op

Due to the vast number of false positives, this labeler is currently no-op, meaning that it does not
publish any labels. Instead, it keeps an internal list of users who are marked as not-suitable,
and provides a feed that blocks those users.

Also, the internal block list is now based on ratio of NSFW/sensitive contents, instead of "oneshot"
block on sight, so the rate of false positives is expected to be lower.

## The feed

With the no-op labeler, this program now focuses on providing a feed that blocks NSFW/sensitive contents.
Have a look at [@oneshot.iroiro.party] for an example.

[@oneshot.iroiro.party]: https://bsky.app/profile/oneshot.iroiro.party/feed/oneshot

This feed uses the following filters to block users and posts:

- Users who are in the internal block list
- Users who are in the external block list
  - This list is stored in a CSV file and can be updated programmatically.
  - With proper config in `.env`, you can report unwanted posts to the no-op labeler,
    and the labeler will add the poster to the external block list automatically.
  - By attaching `del` as the reason to the report, the labeler will remove only the post
    without blocking the user.
- A bunch of user-customized filters at [`feed_filter_user.go`], including:
  - Language filter (using post metadata)
  - Language filter (using the `lingua` library in case the metadata is wrong)
  - Tag filter
  - Keyword filter
  - Rate limiter (to prevent spamming)
  - NSFW/sensitive image filter (using the `vit-base-nsfw-detector` model)
    - You will need to set up [`nsfw-vit/`](./pythonic/nsfw-vit/README.md) for this.

[`feed_filter_user.go`]: ./internal/listener/feed_filter_user.go

## Getting Started

These instructions will get you a copy of the project up and running on your local machine
for development and testing purposes.

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

Configure the feed filters at [`feed_filter_user.go`].

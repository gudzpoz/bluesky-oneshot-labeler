# nsfw-vit

This is a tiny Python wrapper around the [AdamCodd/vit-base-nsfw-detector] model
running on Hugging Face's [transformers] library.

[AdamCodd/vit-base-nsfw-detector]: https://huggingface.co/AdamCodd/vit-base-nsfw-detector
[transformers]: https://huggingface.co/transformers

It is intended to be used only for `bluesky-oneshot-labeler` to detect and remove NSFW posts
and has zero security guarantees.

## Usage

Install [PDM], and run `pdm install` in the root of this repository.
After that:
1. Run `pdm run nsfw-vit preload` to download the model weights.
2. Run `pdm run nsfw-vit serve` to run the server.

[PDM]: https://pdm-project.org/

## API

Send POST requests to the server (any path is fine) with the image URLs in the body,
separated by `\n`. The response will be a JSON array of the same length as the input,
containing the NSFW probability for each image (string members are error messages):

```typescript
({
  nsfw: number,
  sfw: number,
} | string)[]
```

Use the accompanying [`user script`](./test_client.js) to send requests from the browser
and display the results next to the images.

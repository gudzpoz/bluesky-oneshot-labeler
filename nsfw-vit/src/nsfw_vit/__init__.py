import typing

import torch
from PIL import Image
from transformers import (
    AutoModelForImageClassification,
    ViTForImageClassification,
    ViTImageProcessor,
)

class NsfwResult(typing.TypedDict):
    nsfw: float
    sfw: float

class NsfwVitDetector:
    def __init__(self, model_name="AdamCodd/vit-base-nsfw-detector"):
        self.processor = ViTImageProcessor.from_pretrained(model_name)
        self.model = typing.cast(
            ViTForImageClassification,
            AutoModelForImageClassification.from_pretrained(model_name),
        )
        assert dict(
            self.model.config.id2label,
        ) == {
            0: 'sfw',
            1: 'nsfw',
        }, f'model must be trained for nsfw/sfw detection: {self.model.config.id2label}'

    def labels(self) -> dict[int, str]:
        return self.model.config.id2label

    def detect(self, image: list[Image.Image]):
        inputs = self.processor(images=image, return_tensors='pt')
        outputs = self.model(**inputs)
        logits = typing.cast(torch.Tensor, outputs.logits)
        return [
            NsfwResult(nsfw=float(nsfw), sfw=float(sfw))
            for sfw, nsfw in logits
        ]

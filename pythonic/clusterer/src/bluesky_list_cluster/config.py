import dataclasses
import json
from pathlib import Path


@dataclasses.dataclass
class Config:
    config_dir: Path

    user: str
    password: str
    session_file: str
    cache_db: str

    blocked_csv: str
    output_csv: str
    page_rank_damping: float
    rank_threshold: float
    rate_limit: int
    max_followers: int
    depth: int

    @classmethod
    def load(cls, config_file: str | Path):
        with open(config_file, 'r') as f:
            return cls(
                config_dir=Path(config_file).parent,
                **json.load(f),
            )

    @property
    def cache_db_path(self):
        return self.config_dir / self.cache_db

    @property
    def session_file_path(self):
        return self.config_dir / self.session_file

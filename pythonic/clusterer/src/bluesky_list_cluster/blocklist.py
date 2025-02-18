import csv
import logging
import typing
from dataclasses import dataclass
from pathlib import Path

from atproto_client.models import ComAtprotoModerationDefs


_logger = logging.getLogger(__name__)
_info = _logger.info


BAD_REASON_TYPES: set[typing.Union[
    ComAtprotoModerationDefs.ReasonMisleading,
    ComAtprotoModerationDefs.ReasonRude,
    ComAtprotoModerationDefs.ReasonSexual,
    ComAtprotoModerationDefs.ReasonSpam,
    ComAtprotoModerationDefs.ReasonViolation,
]] = {
    'com.atproto.moderation.defs#reasonMisleading',
    'com.atproto.moderation.defs#reasonRude',
    'com.atproto.moderation.defs#reasonSexual',
    'com.atproto.moderation.defs#reasonSpam',
    'com.atproto.moderation.defs#reasonViolation',
}


@dataclass
class BlockListItem:
    index: int
    did: str
    reason_type: str
    reason: str

    def merge_with(self, reason_kind: str, reason: str):
        if reason:
            if self.reason:
                self.reason += ','
            if self.reason_type:
                if reason_kind and reason_kind != self.reason_type:
                    self.reason += f'({reason_kind})'
            self.reason += reason
        self.reason_type = self.reason_type or reason_kind


class BlockList(dict[str, BlockListItem]):
    def __init__(self, path: str | Path, default_bad: bool = True):
        super().__init__()
        self.path = path
        self.last_index = self._read_blocklist(path)
        self.default_bad = default_bad

    def _read_blocklist(self, path: str | Path):
        i = 0
        with open(path, newline='') as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if len(row) == 0 or not row[0].startswith('did:'):
                    continue
                if len(row) == 3:
                    reason_kind = row[1]
                    reason = row[2]
                else:
                    reason_kind = ''
                    reason = ','.join(s for s in row[1:] if s)
                item = BlockListItem(i, row[0], reason_kind, reason)
                if item.did in self:
                    _info('merging blocklist item: %s', item.did)
                    self[item.did].merge_with(item.reason_type, item.reason)
                else:
                    self[item.did] = item
        return i + 1

    def write(self):
        with open(self.path, 'w', newline='') as f:
            writer = csv.writer(f)
            for item in sorted(self.values(), key=lambda x: x.index):
                writer.writerow([item.did, item.reason_type, item.reason])

    def add(self, did: str, reason_type: str, reason: str):
        if did in self:
            self[did].merge_with(reason_type, reason)
        else:
            self[did] = BlockListItem(self.last_index, did, reason_type, reason)
            self.last_index += 1

    def bad_dids(self) -> set[str]:
        return {
            item.did for item in self.values()
            if (
                item.reason_type in BAD_REASON_TYPES
                or (self.default_bad and item.reason_type == '')
            )
        }

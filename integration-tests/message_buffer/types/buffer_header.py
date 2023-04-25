from __future__ import annotations
import typing
from dataclasses import dataclass
from construct import Container
import borsh_construct as borsh


class BufferHeaderJSON(typing.TypedDict):
    bump: int
    version: int
    header_len: int
    end_offsets: list[int]


@dataclass
class BufferHeader:
    layout: typing.ClassVar = borsh.CStruct(
        "bump" / borsh.U8,
        "version" / borsh.U8,
        "header_len" / borsh.U16,
        "end_offsets" / borsh.U16[255],
    )
    bump: int
    version: int
    header_len: int
    end_offsets: list[int]

    @classmethod
    def from_decoded(cls, obj: Container) -> "BufferHeader":
        return cls(
            bump=obj.bump,
            version=obj.version,
            header_len=obj.header_len,
            end_offsets=obj.end_offsets,
        )

    def to_encodable(self) -> dict[str, typing.Any]:
        return {
            "bump": self.bump,
            "version": self.version,
            "header_len": self.header_len,
            "end_offsets": self.end_offsets,
        }

    def to_json(self) -> BufferHeaderJSON:
        return {
            "bump": self.bump,
            "version": self.version,
            "header_len": self.header_len,
            "end_offsets": self.end_offsets,
        }

    @classmethod
    def from_json(cls, obj: BufferHeaderJSON) -> "BufferHeader":
        return cls(
            bump=obj["bump"],
            version=obj["version"],
            header_len=obj["header_len"],
            end_offsets=obj["end_offsets"],
        )

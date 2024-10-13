import numpy as np
import mmh3


class BloomFilter:
    def __init__(self, filter_size: int):
        self.filter_size = filter_size
        self._dtype_bit_size = 8
        num_cells = int(np.ceil(filter_size / self._dtype_bit_size))
        self.is_hashed = np.zeros(num_cells, dtype=np.uint8)

    def _insert_bit(self, pos: int):
        cell_idx = pos // self._dtype_bit_size
        bit_idx = pos % self._dtype_bit_size
        logic_bit = 1 << bit_idx

        self.is_hashed[cell_idx] = np.bitwise_or(self.is_hashed[cell_idx], logic_bit)

    def _get_bit(self, pos: int) -> bool:
        cell_idx = pos // self._dtype_bit_size
        bit_idx = pos % self._dtype_bit_size
        logic_bit = 1 << bit_idx

        tagret_bit = np.bitwise_and(self.is_hashed[cell_idx], logic_bit)
        return tagret_bit > 0

    def hash(self, string: str) -> int:
        return mmh3.hash(string) % self.filter_size

    def put(self, string: str):
        string_hash = self.hash(string)
        self._insert_bit(string_hash)

    def get(self, string: str) -> bool:
        string_hash = self.hash(string)
        return self._get_bit(string_hash)

    def size(self) -> int:
        return sum(bits.bit_count() for bits in self.is_hashed)
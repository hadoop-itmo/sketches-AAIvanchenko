from typing import Generator

import numpy as np
import mmh3


class BloomFilterNHash:
    def __init__(
            self, 
            hash_num: int, 
            filter_size: int
            ):
        self.hash_num = hash_num
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

    def hash(self, string: str) -> Generator:
        for bf_seed in range(0, self.hash_num):
            yield mmh3.hash(string, seed=bf_seed) % self.filter_size

    def put(self, string: str):
        string_hashes = self.hash(string)
        for string_hash in string_hashes:
            self._insert_bit(string_hash)

    def get(self, string: str) -> bool:
        string_hashes = self.hash(string)
        for string_hash in string_hashes:
            if not self._get_bit(string_hash):
                return False
        return True

    def size(self) -> int:
        num_bits = sum(bits.bit_count() for bits in self.is_hashed)
        return num_bits/self.hash_num
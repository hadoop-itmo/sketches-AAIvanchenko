import numpy as np
import mmh3

from typing import Generator


class ConutersBloomFilter:
    def __init__(
            self, 
            hash_num: int, 
            filter_size: int,
            counter_num: int,
            count_thres: int = 1
        ):
        self.hash_num = hash_num
        self.filter_size = filter_size
        self.counter_num = counter_num
        self.count_thres = count_thres
        self._dtype_bit_size = 63
        self._counter_size = counter_num.bit_length()
        self._counter_in_cell = self._dtype_bit_size // self._counter_size
        self._counter_mask = (1 << self._counter_size) - 1
        num_cells = int(np.ceil(filter_size / self._counter_in_cell))
        self.counter_celled_bits = np.zeros(num_cells, dtype=np.int64)

    
    def _add_counter(self, counter_idx: int):
        cell_idx = counter_idx // self._counter_in_cell
        start_bit_idx = (counter_idx % self._counter_in_cell) * self._counter_size
        logic_mask = self._counter_mask << start_bit_idx
        
        # Получим нужные биты счётчика
        curent_counter_val = self.counter_celled_bits[cell_idx] & logic_mask
        add_counter_val = curent_counter_val
        # Если счетчик не переполнен
        if curent_counter_val != logic_mask:
            # Добавим в счетчик бит
            add_counter_val = curent_counter_val + (1 << start_bit_idx)

        self.counter_celled_bits[cell_idx] = np.bitwise_or(self.counter_celled_bits[cell_idx] & ~logic_mask, add_counter_val)

    def _get_counter(self, counter_idx: int) -> int:
        cell_idx = counter_idx // self._counter_in_cell
        start_bit_idx = (counter_idx % self._counter_in_cell) * self._counter_size
        logic_mask = self._counter_mask << start_bit_idx

        tagret_bits = self.counter_celled_bits[cell_idx] & logic_mask
        tagret = tagret_bits >> start_bit_idx
        return tagret

    def hash(self, string: str) -> Generator:
        for bf_seed in range(0, self.hash_num):
            yield mmh3.hash(string, seed=bf_seed) % self.filter_size 

    def put(self, string: str):
        string_hashes = self.hash(string)
        for string_hash in string_hashes:
            self._add_counter(string_hash)

    def get(self, string: str) -> bool:
        string_hashes = self.hash(string)
        for string_hash in string_hashes:
            if self._get_counter(string_hash) < self.count_thres:
                return False
        return True

    def size(self) -> int:
        num_bits = sum(self._get_counter(idx) for idx in range(self.filter_size))
        return num_bits/self.hash_num
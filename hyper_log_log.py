import numpy as np
import mmh3

class HyperLogLog:
    def __init__(self, b: int):
        self.b = b
        self.m = 1 << b # 2**b
        self._hash_size = 32  # in bits
        # Получим кол-во младших битов, выделенных под вычисление ранга
        self._rang_bits_size = self._hash_size - self.b
        self.registers = np.zeros(self.m, dtype=np.uint8)

    @staticmethod
    def get_alpha(m :int) -> int:
        if m == 16:
            return 0.673
        elif m == 32:
            return 0.697
        elif m == 64:
            return 0.709
        else:
            return (0.7213 / (1 + 1.079 / m))

    @staticmethod
    def hash(string: str):
        return mmh3.hash(string, signed=False)

    def hash_info(self, hash_value: int) -> tuple[int, int]:  # index, rang
        # Получим старшие биты индексы путём удаления младших битов ранга
        index_bits: int = hash_value >> self._rang_bits_size
        # Получим младшие биты ранга путём удаления старших битов индекса 
        # (если кол-во биты хеша содержат старшие биты индексов, т.к. кол-вл битов числа динамическое с откидыванием старших бит 0)
        rang_bits: int = hash_value << max(0, hash_value.bit_length() - self._rang_bits_size)
        # Преврощаем биты в строку, переворачиваем её и откидываем служебные '0b', после чего ищем номер первого вхождения 1 в строку
        rang = bin(rang_bits)[:1:-1].find('1') + 1
        # Если '1' в строке не найдено (find вернул -1)
        if rang == 0:
            # Присвоим в качестве ранга максимально возможный ранг
            rang = self._rang_bits_size

        return index_bits, rang
        
    def put(self, string: str):
        hash_value = self.hash(string)
        index, rang = self.hash_info(hash_value)
        self.registers[index] = rang

    def est_size(self):
        # гармоническое среднее
        aplha = self.get_alpha(self.m)
        sum_register = np.power(2, -self.registers.astype(np.float32)).sum()
        # print(sum_register)
        E = (aplha * self.m**2)/sum_register

        if E <= 5 / 2 * self.m:
            V = np.sum(self.registers == 0)
            #small range correction
            if V != 0:
                E = self.m * np.log10(self.m / V)
        elif E > (1 / 30) * (1 << 32):
            # large range correction
            E = -(1 << 32) * np.log10(1 - E / (1 << 32))

        return E
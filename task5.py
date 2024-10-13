import random
import uuid

from tqdm import tqdm

from counter_bloom_filter import ConutersBloomFilter


def gen_grouped_seq(name, pattern, *, n_extra_cols=0, to_shuffle=False):
    """
    Порождает файл с заданным шаблоном распределением повторяемости ключей в первом поле

    Шаблон - список пар положительных целых

    Первое число - сколько групп записей с заданной численностью хочется  породить
    Второй число - численность

    Проще объяснить на примерах:

    [(1, 1)] - хотим породить одну запись
    [('key_1', 1)] - хотим породить одну запись с ключем 'key_1'
    [(1, 100)] - хотим породить 100 записей с одним ключом
    [(100, 1)] - хотим породить 100 записей с разными ключами (100 групп записей численностью 1 каждая) 
    [(15, 10)] - хотим породить 10 записей с одним ключом, 10 другим - и так 15 раз
    [(1000, 1), (2, 400)] - хотим породить 1000 записей с уникальными ключами, 400 записей с другим и еще 400 с каким-то еще
    """
    def gen():
        num = 0
        for keys, n_records in pattern:
            if isinstance(keys, int):
                keys=range(keys)
            else:
                keys = [keys]
            for key in keys:
                if isinstance(key, int):
                    body = f"{key + num}:{uuid.uuid4()}"
                else:
                    body = str(key)
                for i2 in range(n_records):
                    for j in range(n_extra_cols):
                        body += f",{uuid.uuid4()}"
                    yield body
            num += len(keys)

    if to_shuffle:
        data = list(gen())
        random.shuffle(data)
        result = data
    else:
        result = gen()

    total = sum(keys*n_records if isinstance(keys, int) else n_records for keys, n_records in pattern)
    with open(name, "wt") as f:
        for v in tqdm(result, total=total):
            print(v, file=f)


def read_csv_keys(csv_name):
    """ Считаем первую колонку из csv файла"""
    with open(csv_name, 'rt') as f:
        for line in f:
            key = line.rstrip().split(',')[0]
            yield key


def count_keys(keys_iter, counter_bf: ConutersBloomFilter,
               sup_counter_bf: ConutersBloomFilter = None,
                return_keys: bool = False) -> set:
    thres_keys = set()

    # Пройдёмся по всем ключам
    for key in keys_iter:
        # Если вспомогательный фильтр не определен 
        # или ключ встречается достаточное кол-во раз во вспомогательном фильтре
        if sup_counter_bf is None or sup_counter_bf.get(key):
            # Добавим ключ в основной фильтр
            counter_bf.put(key)
            # Если в основном фильтре их набралось пороговое кол-во - запомним
            if return_keys and counter_bf.get(key):
                old_len = len(thres_keys)
                thres_keys.add(key)
                if len(thres_keys) - old_len:
                    print('Add new key:', key)
    
    return thres_keys


if __name__ == '__main__':
    target_table_row_num = 10**6

    repeated_nums = [55000, 60000, 65000, 70000, 75000, 80000]
    repeated_keys = [f"{key}:{uuid.uuid4()}" for key in range(len(repeated_nums))]
    repeated_patterns_1 = [(key, num) for key, num in zip(repeated_keys, repeated_nums)]
    repeated_patterns_2 = [(key, num) for key, num in zip(repeated_keys, reversed(repeated_nums))]

    print('repeated_patterns in "task5_1.csv":', repeated_patterns_1)
    print('repeated_patterns in "task5_2.csv":', repeated_patterns_2)

    gen_grouped_seq('task5_1.csv', [*repeated_patterns_1, (target_table_row_num - sum(repeated_nums), 1)])
    gen_grouped_seq('task5_2.csv', [*repeated_patterns_2, (target_table_row_num - sum(repeated_nums), 1)])


    filter_size = target_table_row_num // 10
    counter_num = count_thres = 60000
    hash_num = 5  # т.к. оптимальное кол-во 1200, возьмем меньше для экономии ресурсов

    # Создадим ConutersBloomFilter для первого файла
    file_1_counter_bf = ConutersBloomFilter(filter_size=filter_size, hash_num=hash_num, counter_num=counter_num, count_thres=count_thres)
    # Создадим ConutersBloomFilter для второго файла
    # Кол-во добавленных уникальных ключей будет меньше, чем для первого файла, поэтому filter_size должно хватать для записи всех ключей без ложных срабатываний
    file_2_counter_bf = ConutersBloomFilter(filter_size=filter_size, hash_num=hash_num, counter_num=counter_num, count_thres=count_thres)

    # Посчитаем кол-во ключей в первом файле с помощью ConutersBloomFilter
    first_key_iter = tqdm(
        read_csv_keys('task5_1.csv'),
        desc='Collect 1 file',
        total=target_table_row_num
    )
    count_keys(first_key_iter, counter_bf=file_1_counter_bf)

    # Пройдёмся по второму файлу, зафиксируем ключи, которые встречаются нужное количество раз и запомним их
    second_key_iter = tqdm(
        read_csv_keys('task5_2.csv'), 
        desc='Collect 2 file',
        total=target_table_row_num
    )
    second_thres_keys = count_keys(second_key_iter, counter_bf=file_2_counter_bf, sup_counter_bf=file_1_counter_bf, return_keys=True)

    print(f'Repeated > {count_thres} keys:', second_thres_keys)


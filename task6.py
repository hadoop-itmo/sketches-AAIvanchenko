from collections import defaultdict

from tqdm.auto import tqdm

from counter_bloom_filter import ConutersBloomFilter


def count_join_size(
        table_1_keys, 
        table_2_keys, 
        saved_1_table_keys,  # Сохранять ключи можно в процессе работы алгоритма, для упрощения просто пройдёмся по файлу заново
        unique_key_thres = 10**6,  # Макс. кол-во уникальных ключей для точного метода,
        join_row_thres = 10**7,  # Макс. кол-во строк в join, после которого перестаём считать
        max_unique_key_size = 10**8  # Мах. кол-во уникальных ключей
        ):
    
    hash_num = 4
    filter_size = max_unique_key_size  # Мах. кол-во уникальных ключей
    counter_num = 15  # Размер счётчика исходя из того, что каждый ключ повторяется ~10 раз

    # Фильтры дл неточного подсчета
    table_1_counter_bf = ConutersBloomFilter(
        hash_num=hash_num,
        filter_size=filter_size,
        counter_num=counter_num,
    )
    table_2_counter_bf = ConutersBloomFilter(
        hash_num=hash_num,
        filter_size=filter_size,
        counter_num=counter_num,
    )
    # Словарь для точного подсчета
    table_1_count_dict = defaultdict(lambda: 0)
    table_2_count_dict = defaultdict(lambda: 0)

    # Пройдёмся по каждому ключу из 2 таблиц
    keys_enable = True
    table_1_key_iter = iter(table_1_keys)
    table_2_key_iter = iter(table_2_keys)
    p_bar = tqdm(desc='Process 1 and 2 file')
    while keys_enable:
        keys_enable = False

        for key_iter, counter_bf, counter_dict in zip(
            [table_1_key_iter, table_2_key_iter],
            [table_1_counter_bf, table_2_counter_bf],
            [table_1_count_dict, table_2_count_dict]
        ):
            try:
                # Получим ключ первой таблицы
                key = next(key_iter)
                keys_enable = True
                # Запомним ключ для приблизительного подсчета
                counter_bf.put(key)
                if counter_dict is not None:
                    # Запомним ключ для точного подсета
                    counter_dict[key] += 1
            except StopIteration:
                pass

        # Если уникальных ключей больше порога - удалим точную реализацию
        if table_1_count_dict is not None and any(len(c_dict) > unique_key_thres for c_dict in [table_1_count_dict, table_2_count_dict]):
            table_1_count_dict = None
            table_2_count_dict = None
        
        p_bar.update(1)

    join_row_counts = 0
    # Если точная реализация
    if table_1_count_dict is not None:
        print('Use accurate algorythm')
        # Пройдёмся по значениям первой таблицы
        for key, table_1_key_count in table_1_count_dict.items():
            # Получим соответствующее значение второй таблицы
            table_2_key_count = table_2_count_dict[key]
            # Перемножим значения для получения кол-ва строк по данному ключу
            join_row_counts += table_1_key_count * table_2_key_count
    # Если неточная реализация
    else:
        print('Use non-accurate algorythm')
        # Заново пройдёмся по всем ключам из одного из файла
        saved_1_table_key_iter = tqdm(saved_1_table_keys, desc='Counting JSON rows by non-accurate algorythm')
        for key in saved_1_table_key_iter:
            # Получим значение встречаемости во втором файле
            # как среднее значение счётчиков и поделим на количество хешей -> Примерное кол-во одинаковых комбинаций
            counter_idxes = table_2_counter_bf.hash(key)
            table_2_comb_count = int(
                min(table_2_counter_bf._get_counter(c_idx) for c_idx in counter_idxes) / hash_num
            )
            # Перемножим соответствующие значения комбинаций (каждый счетчик отвечает за свой ключ)        
            join_row_counts += table_2_comb_count
            # Если значения строк больше порога - перестанем считать
            if join_row_counts > join_row_thres:
                print(f'JOIN rows > {join_row_thres}')
                break
    
    # Вернём значение строк
    return join_row_counts
            
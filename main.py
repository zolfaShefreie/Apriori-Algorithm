import pandas as pd
import numpy as np
import itertools
from multiprocessing import Pool


def convert_csv_to_set(path: str) -> set:
    """
    this function convert a csv file to set for get all values of the table
    :param path: the path of a file with .csv format
    :return: a set of values without np.nan
    """
    df = pd.read_csv(path, header=None)

    np_array = df.to_numpy()
    np_array = np_array.flatten().tolist()
    items = set(np_array)
    items.discard(np.nan)

    return items


def convert_csv_to_dict_data(path: str) -> dict:
    """
    this function convert a csv file to and dict
    :param path: the path of a file with .csv format
    :return: return  a dictionary with key: index and value a set of sell items without np.nan
    """
    df = pd.read_csv(path, header=None)

    dict_data = df.T.to_dict('list')
    for each in dict_data:
        set_data = set(dict_data[each])
        set_data.discard(np.nan)
        dict_data[each] = set_data

    return dict_data


class Rules:
    def __init__(self, rule_part_a: dict, rule_part_b: dict, a_plus_b: int, max_trans: int, sort_by="lift"):
        self.rule_part_a = rule_part_a
        self.rule_part_b = rule_part_b
        self.max_transactions = max_trans
        self.sup_a_plus_b = a_plus_b
        self.sort_by = sort_by
        self.sup = self.calculate_sup(a_plus_b, max_trans)
        self.conf = self.calculate_conf(list(rule_part_a.values())[0], a_plus_b)
        self.lift = self.calculate_lift(self.conf, list(rule_part_b.values())[0] / max_trans)

    @staticmethod
    def calculate_sup(sup_count_a_plus_b: int, max_trans: int):
        return sup_count_a_plus_b / max_trans

    @staticmethod
    def calculate_conf(sup_count_a: int, sup_count_a_plus_b: int):
        return sup_count_a_plus_b / sup_count_a

    @staticmethod
    def calculate_lift(conf: float, sup_b: float):
        return conf / sup_b

    def __le__(self, other):
        if self.sort_by == "lift":
            return self.lift < other.lift
        elif self.sort_by == "support":
            return self.sup < other.sup
        elif self.sort_by == "confidence":
            return self.conf < other.conf
        return False


class Arules:
    MAX_LENGTH = 1000

    def __init__(self):
        self.continue_level = True
        self.l = []

    @staticmethod
    def get_items(transactions: dict) -> set:
        """
        get the items of transactions
        :param transactions: a dict {transaction_id: set of items}
        :return: set of items
        """
        items = set()
        for each in transactions.values():
            items.update(each)
        return set(items)

    def level_process(self, transactions: dict, level: int, min_sup: float):
        args = [(transactions[i*self.MAX_LENGTH: (i+1)*self.MAX_LENGTH], level) for i in range(self.MAX_LENGTH)]
        pool = Pool(int(len(transactions) / self.MAX_LENGTH) + 1)
        c_results = self.merge_dicts(pool.starmap(self.get_c_dict, args))
        if level == 1:
            self.l.append(self.get_l_dict(len(transactions), c_results, min_sup, level))
        else:
            self.l.append(self.get_l_dict(len(transactions), c_results, min_sup, level, self.l[level-1]))

    def get_c_dict(self, transactions: dict, level: int) -> dict:
        """
        :param transactions: the dict of transactions
        :param level: the level of calculate
        :return: a dict {item: sup}
        """
        if len(self.l) > 0:
            items = set(self.l[0].keys())
        else:
            items = self.get_items(transactions)

        key_list = list(map(set, itertools.combinations(set(items), level)))
        result_dict = dict()
        for each in key_list:
            for transaction in transactions.values():
                if transaction.intersection(each) == each:
                    result_dict[each] = result_dict.get(each, 0) + 1
        return result_dict

    @staticmethod
    def get_l_dict(max_length: int, c: dict, min_sup: float, level: int, pre_l=None) -> dict:
        """
        :param max_length: number of transactions
        :param c: the return of get_c_dict
        :param min_sup: a float number between 0 and 1
        :param level:
        :param pre_l: a dict for self.c[level-1]
        :return: a dict with valid sup
        """
        for each in c:
            if len(each) > 1:
                sub_keys = list(map(set, itertools.combinations(set(each), level-1)))
                if pre_l is None:
                    raise Exception
                pre_keys = set(pre_l.keys())
                commons = pre_keys.intersection(sub_keys)
                if len(commons) != len(sub_keys):
                    c.pop(each)
                    continue
            if c[each] / max_length < min_sup:
                c.pop(each)
        return c

    @staticmethod
    def merge_dicts(list_dict: list) -> dict:
        """
        if a key in dict_a and dict_b => value of key = dict_b[key] + dict_a[key]
        :param list_dict:  list of {key: value} that value must be int
        :return: merge dict
        """
        result = dict()
        keys = list(set([key for each in list_dict for key in each]))
        for key in keys:
            value = 0
            for each in list_dict:
                value += each.get(key, 0)
            result[key] = value
        return result

    def get_frequent_item_sets(self, transactions: dict, min_sup: float) -> list:
        """
        :param transactions:
        :param min_sup:
        :return: the list of last level
        """
        level = 1
        while self.continue_level:
            self.level_process(transactions, level, min_sup)
            if not self.l[level-1]:
                self.continue_level = False
            else:
                level += 1
        return list(self.l[level-2].keys())

    def get_arules(self, min_sup=None, min_conf=None, min_lift=None, sort_by='lift'):
        pass

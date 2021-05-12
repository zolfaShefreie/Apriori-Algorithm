import pandas as pd
import numpy as np
import itertools
from multiprocessing import Pool
import time


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
    # dict_data = dict()
    # pool = Pool(11)
    # results = pool.map(convert_dataframe_to_dict, [df[i*100: (i+1)*100] for i in range(99)])
    # for each in results:
    #     dict_data.update(each)
    dict_data = df.T.to_dict('list')
    for each in dict_data:
        set_data = set(dict_data[each])
        set_data.discard(np.nan)
        dict_data[each] = set_data

    return dict_data


def convert_dataframe_to_dict(df: pd.DataFrame) -> dict:
    dict_data = df.T.to_dict('list')
    for each in dict_data:
        set_data = set(dict_data[each])
        set_data.discard(np.nan)
        dict_data[each] = set_data
    return dict_data


class Rule:
    def __init__(self, rule_part_a: dict, rule_part_b: dict, a_plus_b: int, max_trans: int):
        self.rule_part_a = rule_part_a
        self.rule_part_b = rule_part_b
        self.max_transactions = max_trans
        self.sup_a_plus_b = a_plus_b
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
        if self.rule_part_a < other.rule_part_a:
            return True
        return self.rule_part_a == other.rule_part_a and self.rule_part_b < other.rule_part_b

    def __eq__(self, other):
        if self.rule_part_a == other.rule_part_a and self.rule_part_b == other.rule_part_b:
            return True
        return False

    def __str__(self):
        return "{}->{}".format(self.rule_part_a, self.rule_part_b)

    @staticmethod
    def sort_by(elem, sort_by):
        if sort_by == "lift":
            return elem.lift
        if sort_by == "support":
            return elem.support
        elif sort_by == "confidence":
            return elem.conf

        return elem.lift


class Arules:
    MAX_LENGTH = 1000
    MAX_ITEM_SET_RESULT = 10

    def __init__(self):
        self.continue_level = True
        self.max_transactions = 0
        self.l = []
        self.level_items = {}

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
        """
        a func management process of one level
        :param transactions:
        :param level: level or depth of process
        :param min_sup: minimum support
        :return:
        """
        item_keys = self.get_level_item_keys(level)
        args = [(dict(list(transactions.items())[i*self.MAX_LENGTH: (i+1)*self.MAX_LENGTH]), level, item_keys)
                for i in range(int(len(transactions) / self.MAX_LENGTH) + 1)]
        pool = Pool(int(len(transactions) / self.MAX_LENGTH) + 1)
        results = pool.starmap(self.get_c_dict, args)
        c_results = self.merge_dicts(results)
        if level == 1:
            self.l.append(self.get_l_dict(len(transactions), c_results, min_sup, level))
        else:
            self.l.append(self.get_l_dict(len(transactions), c_results, min_sup, level, self.l[level-2]))

    def get_level_item_keys(self, level: int):
        """
        :param level: level or depth of process
        :return: the key items
        """
        key_list = None
        if len(self.l) > 0:
            items = list(self.l[level - 2].keys())
            key_list = set()
            count = 1
            for i in range(len(items)):
                item = sorted(list(items[i]))
                for each in items[i + 1:]:
                    each = sorted(list(each))
                    if item[:-1] == each[:-1] and item[-1] != each[-1]:
                        key_list.add(frozenset(item + [each[-1], ]))
                    count += 1
            # TODO delete print
            print(len(key_list), "(((((((((((((((((((((((")
        return key_list

    def get_c_dict(self, transactions: dict, level: int, item_keys=None) -> dict:
        """
        make the table c
        :param transactions: the dict of transactions
        :param level: level or depth of process
        :param item_keys: a list of tuple for a set of items
        :return: a dict {item: sup}
        """
        if level == 1:
            items = self.get_items(transactions)
            item_keys = list(map(frozenset, itertools.combinations(set(items), level)))

        result_dict = dict()
        for each in item_keys:
            for transaction in transactions.values():
                if transaction.intersection(each) == set(each):
                    result_dict[each] = result_dict.get(each, 0) + 1
        return result_dict

    @staticmethod
    def get_l_dict(max_length: int, c: dict, min_sup: float, level: int, pre_l=None) -> dict:
        """
        make the table l
        :param max_length: number of transactions
        :param c: the return of get_c_dict
        :param min_sup: a float number between 0 and 1
        :param level: level or depth of process
        :param pre_l: a dict for self.c[level-1]
        :return: a dict with valid sup
        """
        c_copy = dict(c)
        for each in c:
            if len(each) > 1:
                sub_keys = list(map(frozenset, itertools.combinations(set(each), level-1)))
                if pre_l is None:
                    raise Exception
                pre_keys = set(pre_l.keys())
                commons = pre_keys.intersection(sub_keys)
                if len(commons) != len(sub_keys):
                    c_copy.pop(each)
                    continue
            if c[each] / max_length < min_sup:
                c_copy.pop(each)
        return c_copy

    @staticmethod
    def merge_dicts(list_dict: list) -> dict:
        """
        if a key in dict_a and dict_b => value of key = dict_b[key] + dict_a[key]
        :param list_dict:  list of {key: value} that value must be int
        :return: merge dict
        """
        result = dict()
        keys = list(set([frozenset(key) for each in list_dict for key in each]))
        for key in keys:
            value = 0
            for each in list_dict:
                value += each.get(key, 0)
            result[key] = value
        # TODO delete the print
        print(len(result), 'after merge')
        return result

    def get_frequent_item_sets(self, transactions: dict, min_sup: float) -> list:
        """
        get n item set
        :param transactions: a dict of transaction ids and the list of items
        :param min_sup: minimum support
        :return: the list of last level
        """
        self.max_transactions = len(transactions)
        level = 1
        while True:
            self.level_process(transactions, level, min_sup)
            if not self.l[level-1]:
                # print(level)
                break
            level += 1
        return list(self.l[level-1].keys())[:self.MAX_ITEM_SET_RESULT]

    def get_arules(self, min_sup=float('-inf'), min_conf=float('-inf'), min_lift=float('-inf'), sort_by='lift'):
        """
        get all rules and sort it
        :param min_sup: a float between 0 and 1
        :param min_conf: a float between 0 and 1
        :param min_lift: a float between 0 and 1
        :param sort_by: choices = (lift, support, confidence)
        :return: sorted rules
        """
        item_sets = list(self.l[len(self.l)-2].keys())
        args = [(set(each), self.l[len(self.l)-2][each], min_sup, min_conf, min_lift) for each in item_sets]
        pool = Pool((len(item_sets)))
        rules = pool.starmap(self.get_item_set_rule, args)
        rules = [rule for each in rules for rule in each]
        return sorted(rules, key=lambda x: Rule.sort_by(x, sort_by))

    def get_item_set_rule(self, item_set: set, sup_count: int, min_sup=float('-inf'), min_conf=float('-inf'),
                          min_lift=float('-inf')) -> list:
        """
        get unsorted rules of one item set
        :param item_set: one set of  frequent item
        :param sup_count: the sup count of item_set in transactions
        :param min_sup: a float between 0 and 1
        :param min_conf: a float between 0 and 1
        :param min_lift: a float between 0 and 1
        :return: a list of rule
        """
        rule_parts_list = list()
        rule_obj_list = list()
        for i in range(int(len(item_set)-1)):
            sub_sets = list(map(set, itertools.combinations(item_set, i+1)))
            for each in sub_sets:
                complement = item_set - each
                if (each, complement) not in rule_parts_list:
                    rule_parts_list.append((each, complement))
                    rule = Rule(rule_part_a={frozenset(each): self.l[len(each)-1][frozenset(each)]},
                                rule_part_b={frozenset(complement): self.l[len(complement)-1][frozenset(complement)]},
                                a_plus_b=sup_count,
                                max_trans=self.max_transactions)
                    if rule.sup >= min_sup and rule.conf >= min_conf and rule.lift >= min_lift:
                        rule_obj_list.append(rule)
        return rule_obj_list


if __name__ == "__main__":
    transactionss = convert_csv_to_dict_data("Book2.csv")
    algo = Arules()
    algo.get_frequent_item_sets(transactionss, 2/9)
    print(algo.l)
    l = algo.get_arules()
    l = [str(each) for each in l]
    print(l)

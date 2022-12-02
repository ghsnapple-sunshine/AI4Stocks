from akshare import stock_board_concept_name_em
from akshare.stock_feature.stock_hist_em import code_id_map_em


class Lazy:
    _code_id_dict = None
    _stock_board_concept_name_map = None
    _adjust_dict = {"qfq": "1", "hfq": "2", "": "0"}
    _period_dict = {"daily": "101", "weekly": "102", "monthly": "103"}

    @classmethod
    def get_code_id_dict(cls) -> dict:
        if cls._code_id_dict is None:
            cls._code_id_dict = code_id_map_em()
        return cls._code_id_dict

    @classmethod
    def get_adjust_dict(cls):
        return cls._adjust_dict

    @classmethod
    def get_period_dict(cls):
        return cls._period_dict

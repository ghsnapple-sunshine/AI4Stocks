from __future__ import annotations

import itertools
import pickle
from importlib import import_module

from buffett.constants.magic import TYPE, MTD, BM, OBJ, DICT, CMPLX, NoneType, ME, SOURCE, LIST, TUPLE, SET, RTYPE, \
    get_name, get_class, get_module_name, get_self


class SerializeTool:
    _SERIALIZE = {(int, float, bool, str, bytes, NoneType): lambda o, c: o,
                  (list, tuple, set):
                      lambda o, c: {TYPE: get_name(c),
                                    OBJ: [SerializeTool._do(v) for v in o]},
                  (dict,):
                      lambda o, c: {TYPE: DICT,  # 等效于 getattr(c, NAME)
                                    OBJ: {} if len(o) == 0 else
                                    dict([(SerializeTool._do(k), SerializeTool._do(v)) for k, v in o.items()])},
                  (complex,):
                      lambda o, c: {TYPE: CMPLX,  # 等效于 getattr(c, NAME)
                                    OBJ: [o.real, o.imag]}}
    _SERIALIZE = dict(itertools.chain.from_iterable(
        [[(ks, v) for ks in k] for k, v in _SERIALIZE.items()]))

    _DESERIALIZE = {LIST: lambda o: [SerializeTool._undo(v) for v in o],
                    TUPLE: lambda o: (SerializeTool._undo(v) for v in o),
                    SET: lambda o: (SerializeTool._undo(v) for v in o),
                    DICT: lambda o: dict([(SerializeTool._undo(k), SerializeTool._undo(v)) for k, v in o.items()]),
                    CMPLX: lambda o: complex(o[0], o[1])}

    @classmethod
    def serialize(cls, obj) -> bytes:
        done_obj = SerializeTool._do(obj)
        return pickle.dumps(done_obj)

    @classmethod
    def deserialize(cls, obj: bytes) -> object:
        undone_obj = pickle.loads(obj)
        return SerializeTool._undo(undone_obj)

    @staticmethod
    def _do(obj):
        obj_cls = get_class(obj)
        if obj_cls in SerializeTool._SERIALIZE:
            return SerializeTool._SERIALIZE[obj_cls](obj, obj_cls)
        # 其他object类型
        otp = {TYPE: get_name(obj_cls),
               SOURCE: get_module_name(obj_cls)}
        otp_obj = {}
        for att_name in dir(obj):
            if att_name.startswith('__'):  # 不处理内建方法
                continue
            obj_attr = getattr(obj, att_name)
            if hasattr(obj_cls, att_name) and getattr(obj_cls, att_name) == obj_attr:  # 不处理类属性
                continue
            elif callable(obj_attr):
                caller = get_self(obj)
                if caller is not None and caller != obj:
                    # 只序列化绑定对象不为自身的绑定方法
                    # 绑定对象不为自身的绑定方法，非绑定方法均不参与序列化
                    otp_obj[att_name] = {TYPE: BM,
                                         OBJ: SerializeTool._do(caller),
                                         MTD: get_name(obj)}
                continue
            elif obj_attr == obj:  # 包含对自身的引用
                otp_obj[att_name] = {TYPE: ME}
            else:
                otp_obj[att_name] = SerializeTool._do(obj_attr)
        otp[OBJ] = otp_obj
        return otp

    @staticmethod
    def _undo(source):
        if not isinstance(source, dict):
            return source
        if SOURCE not in source:
            return {} if len(source) == 0 else SerializeTool._DESERIALIZE[source[TYPE]](source)
        inp_cls = getattr(import_module(source[SOURCE]), source[TYPE])
        inp_cls.__slots__ = ()
        # RType = type(RTYPE, (inp_cls, ), {NEW: NEW_METHOD, INIT: INIT_METHOD})
        # otp = RType()
        runtime_type = type(RTYPE, (object,), {'__slots__': ()})
        otp = runtime_type()
        for att_name, att in source[OBJ].items():
            if att_name == ME:
                setattr(otp, att_name, otp)
            elif att_name == BM:
                sotp = SerializeTool._undo(att)
                mtd = getattr(sotp, MTD)
                setattr(otp, att_name, mtd)
            else:
                out_att = SerializeTool._undo(att)
                setattr(otp, att_name, out_att)

        otp.__class__ = inp_cls
        return otp

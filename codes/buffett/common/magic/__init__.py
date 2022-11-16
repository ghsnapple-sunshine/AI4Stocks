from buffett.common.magic.tools import get_class, get_class_name, get_self, get_module_name, get_name, load_class, \
    empty_init, empty_method

# TODO: About to remove this.
DTYPE = 'DType'
RTYPE = 'RType'  # Runtime Type
#

# type_definition
LIST = get_class(list)
TUPLE = get_class(tuple)
SET = get_class(set)
DICT = get_class(dict)
CMPLX = get_class(complex)
BM = 'bound_method'  #
ME = 'me'
#
TYPE = 'type'
SOURCE = 'source'
MTD = 'method'
OBJ = 'object'
#
NoneType = type(None)
BOUND_METHOD = get_class(getattr(type(DTYPE, (), {'run': lambda x: x})(), 'run'))
METHOD_WRAPPER = get_class(getattr(type(DTYPE, (), {'run': lambda x: x}), 'run'))
FUNCTION = get_class((lambda x: x))

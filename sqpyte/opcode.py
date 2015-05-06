from rpython.rlib.unroll import unrolling_iterable

dual_implementation_opcodes = [
    'Add_Subtract_Multiply_Divide_Remainder',
    'Affinity',
    'CollSeq',
    'Compare',
    'Copy',
    'EndCoroutine',
    'Gosub',
    'Goto',
    'IdxLE_IdxGT_IdxLT_IdxGE',
    'IdxRowid',
    'IfPos',
    'IfZero',
    'If_IfNot',
    'InitCoroutine',
    'Integer',
    'IsNull',
    'Jump',
    'MakeRecord',
    'Move',
    'MustBeInt',
    'Ne_Eq_Gt_Le_Lt_Ge',
    'Next',
    'NextIfOpen',
    'NotExists',
    'NotNull',
    'Null',
    'Once',
    'OpenRead_OpenWrite',
    'Real',
    'RealAffinity',
    'ResultRow',
    'Return',
    'SCopy',
    'Seek',
    'SeekLT_SeekLE_SeekGE_SeekGT',
    'Sequence',
    'Variable',
    'Yield',
]

unrolling_dual_implementation_opcodes = unrolling_iterable(dual_implementation_opcodes)

class OpcodeDefaults(object):
    OpenRead_OpenWrite = False
OpcodeDefaults = OpcodeDefaults()

for op in dual_implementation_opcodes:
    if not hasattr(OpcodeDefaults, op):
        setattr(OpcodeDefaults, op, True)


class OpcodeStatus(object):
    _immutable_fields_ = ["frozen", "use_flag_cache"] + dual_implementation_opcodes

    def __init__(self, use_flag_cache):
        self.use_flag_cache = use_flag_cache
        self.frozen = False
        for op in unrolling_dual_implementation_opcodes:
            setattr(self, op, getattr(OpcodeDefaults, op))

    def set_use_translated(self, op, value):
        if self.frozen:
            raise TypeError("too late to change")
        if self.use_flag_cache:
            raise TypeError("can't change if flag cache is used")
        for whichop in unrolling_dual_implementation_opcodes:
            if whichop == op:
                setattr(self, op, value)
                if whichop == "Compare":
                    self.Jump = value
                elif whichop == "Jump":
                    self.Compare = value

    def freeze(self):
        if not self.frozen:
            self.frozen = True

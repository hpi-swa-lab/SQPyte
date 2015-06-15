from rpython.rlib.unroll import unrolling_iterable

dual_implementation_opcodes = [
    'Add_Subtract_Multiply_Divide_Remainder',
    'AggFinal',
    'AggStep',
    'Affinity',
    'Cast',
    'CollSeq',
    'Compare',
    'Copy',
    'EndCoroutine',
    'Function',
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
    Cast = False
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
                setattr(self, whichop, value)
                if whichop == "Compare":
                    self.Jump = value
                elif whichop == "Jump":
                    self.Compare = value
                elif whichop == "AggStep":
                    self.AggFinal = value
                elif whichop == "AggFinal":
                    self.AggStep = value

    def freeze(self):
        if not self.frozen:
            self.frozen = True

    def disable_from_cmdline(self, s):
        if s == "all":
            for op in unrolling_dual_implementation_opcodes:
                setattr(self, op, False)
            return
        specs = s.split(":")
        for spec in specs:
            if spec:
                self.set_use_translated(spec, False)

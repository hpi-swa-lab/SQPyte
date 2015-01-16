from rpython.rlib import jit, rarithmetic
from rpython.rtyper.lltypesystem import rffi
from sqpyte.capi import CConfig
from sqpyte import capi, interpreter
from rpython.rtyper.lltypesystem import lltype

class FunctionError(interpreter.SQPyteException):
    pass

class Func(object):
    _immutable_fields_ = ["pfunc", "name", "nArg", "contextcls"]

    def __init__(self, name, narg, contextcls=None):
        # will be set before first use
        self.pfunc = lltype.nullptr(capi.FUNCDEFP.TO)
        self.name = name
        self.nArg = narg
        assert not isinstance(contextcls, int)
        self.contextcls = contextcls

    def apArg(self):
        return self.pfunc.apArg

    @jit.elidable
    def exists_in_python(self):
        return self.python_context_class() is not Context

    @jit.elidable
    def python_context_class(self):
        if self.contextcls is not None:
            return self.contextcls
        if self.name == "count":
            return CountCtx
        if self.name == "sum" and self.nArg == 1:
            return SumCtx
        if self.name == "avg" and self.nArg == 1:
            return AvgCtx
        return Context

    @jit.unroll_safe
    def aggstep_in_python(self, hlquery, op, index, numargs):
        cls = self.python_context_class()
        args = [hlquery.mem_with_index(index + i) for i in range(numargs)]
        p = aggregate_context(op.mem_of_p(3), cls, self)
        p.step(args)
        return 0

    def aggfinal_in_python(self, hlquery, op, memout):
        cls = self.python_context_class()
        p = memout._python_ctx
        cls.finalize(p, memout)
        memout._python_ctx = None
        return 0


class FuncRegistry(object):

    def __init__(self):
        self.aggregates = {}
        self.funcs = [None] # dummy func

    def create_aggregate(self, name, numargs, contextcls):
        func = Func(name, numargs, contextcls)
        key = name, numargs
        assert key not in self.aggregates
        self.aggregates[key] = contextcls
        self.funcs.append(func)
        return len(self.aggregates), func

    def get_func(self, pfunc):
        step_as_int = rffi.cast(lltype.Signed, pfunc.xStep)
        if step_as_int == 1: # an rpython-defined function
            assert rffi.cast(lltype.Signed, pfunc.xFinalize) == 1
            index = rffi.cast(lltype.Signed, pfunc.pUserData)
            func = self.funcs[index]
            if func and not func.pfunc:
                func.pfunc = pfunc
            return func
        # XXX what here?
        name = rffi.charp2str(pfunc.zName)
        nArg = rffi.cast(lltype.Signed, pfunc.nArg)
        func = Func(name, nArg)
        func.pfunc = pfunc
        return func

def aggregate_context(mem, cls, func):
    if mem._python_ctx is None:
        mem._python_ctx = cls(func)
    return mem._python_ctx


class Context(object):
    def __init__(self, func):
        self.func = func

class CountCtx(Context):
    """
    The following structure keeps track of state information for the
    count() aggregate function.
    """
    def __init__(self, func):
        self.func = func
        self.n = 0

    def step(self, args):
        if not args or (args[0].sqlite3_value_type() != CConfig.SQLITE_NULL):
            self.n += 1

    @staticmethod
    def finalize(self, memout):
        if self is None:
            res = 0
        else:
            assert isinstance(self, CountCtx)
            res = self.n
        memout.sqlite3_result_int64(res)

class AbstractSumCtx(Context):
    """
    An instance of the following structure holds the context of a
    sum() or avg() aggregate computation.
    """

    def __init__(self, func):
        self.func = func
        self.rSum = 0.0        # Floating point sum
        self.iSum = 0          # Integer sum
        self.cnt = 0           # Number of elements summed
        self.overflow = False  # True if integer overflow seen
        self.approx = False    # True if non-integer value was input to the sum

    def step(self, args):
        """
        Routines used to compute the sum, average, and total.

        The SUM() function follows the (broken) SQL standard which means
        that it returns NULL if it sums over no inputs.  TOTAL returns
        0.0 in that case.  In addition, TOTAL always returns a float where
        SUM might return an integer if it never encounters a floating point
        value.  TOTAL never fails, but SUM might through an exception if
        it overflows an integer.
        """
        arg, = args
        typ = arg.sqlite3_value_numeric_type()
        self.cnt += 1
        if typ == CConfig.SQLITE_INTEGER:
            v = arg.sqlite3_value_int64()
            self.rSum += v
            if not self.approx and not self.overflow:
                try:
                    self.iSum = rarithmetic.ovfcheck(self.iSum + v)
                except OverflowError:
                    self.overflow = True
        else:
            self.rSum += arg.sqlite3_value_double()
            self.approx = True

class SumCtx(AbstractSumCtx):
    @staticmethod
    def finalize(self, memout):
        if not self:
            return
        assert isinstance(self, SumCtx)
        if self.cnt > 0:
            if self.overflow:
                raise FunctionError("integer overflow")
            if self.approx:
                memout.sqlite3_result_double(self.rSum)
            else:
                memout.sqlite3_result_int64(self.iSum)

class AvgCtx(AbstractSumCtx):
    @staticmethod
    def finalize(self, memout):
        if not self:
            return
        assert isinstance(self, AvgCtx)
        if self.cnt > 0:
            memout.sqlite3_result_double(self.rSum / self.cnt)

class DummyCtx(Context):
    pass

from rpython.rtyper.lltypesystem import rffi, lltype
from sqpyte import capi
CConfig = capi.CConfig
from sqpyte.mem import Mem, CacheHolder

class FakeHLQuery(object):
    def __init__(self):
        self.mem_cache = CacheHolder(1)


def test_caching():
    with lltype.scoped_alloc(capi.MEM) as pMem:
        rffi.setintfield(pMem, 'flags', CConfig.MEM_Int)
        pMem.u.i = 17
        pMem.r = 2.3
        mem = Mem(FakeHLQuery(), pMem, 0)
        assert mem.get_flags() == CConfig.MEM_Int
        assert mem.get_u_i() == 17
        assert mem.get_r() == 2.3
        # check caching by taking the pMem away and see whether reading still
        # works
        mem.pMem = None
        assert mem.get_flags() == CConfig.MEM_Int
        assert mem.get_u_i() == 17
        assert mem.get_r() == 2.3
        mem.pMem = pMem

        # invalidate cache
        mem.invalidate_cache()
        rffi.setintfield(pMem, 'flags', CConfig.MEM_Real)
        pMem.u.i = 0
        pMem.r = 4.5
        assert mem.get_flags() == CConfig.MEM_Real
        assert mem.get_u_i() == 0
        assert mem.get_r() == 4.5

        # use setters
        mem.set_flags(CConfig.MEM_Null)
        mem.set_u_i(-5)
        mem.set_r(-0.1)
        mem.pMem = None
        assert mem.get_flags() == CConfig.MEM_Null
        assert mem.get_u_i() == -5
        assert mem.get_r() == -0.1
        mem.pMem = pMem
        mem.invalidate_cache()
        assert mem.get_flags() == CConfig.MEM_Null
        assert mem.get_u_i() == -5
        assert mem.get_r() == -0.1


def test_dont_do_superfluous_flag_writes():
    with lltype.scoped_alloc(capi.MEM) as pMem:
        rffi.setintfield(pMem, 'flags', CConfig.MEM_Int)
        pMem.u.i = 17
        pMem.r = 2.3
        mem = Mem(FakeHLQuery(), pMem, 0)
        assert mem.get_flags() == CConfig.MEM_Int
        mem.pMem = None
        mem.set_flags(CConfig.MEM_Int) # works, because unnecessary

def test_cache_on_write():
    class FakeMem(object):
        flags = CConfig.MEM_Int
        r = 12
        class u:
            pass
    pMem = FakeMem()
    mem = Mem(FakeHLQuery(), pMem, 0)
    mem.set_u_i(14) # does not crash
    assert pMem.u.i == 14

    mem.pMem = None
    assert mem.get_u_i() == 14


def test_track_constants():
    class FakeMem(object):
        flags = CConfig.MEM_Int
        r = 12
        class u:
            pass
    hlquery = FakeHLQuery()
    pMem = FakeMem()
    mem = Mem(hlquery, pMem, 0)
    mem.set_u_i(14, constant=True)
    assert mem.is_constant_u_i()
    assert pMem.u.i == 14
    hlquery.integers = None
    assert mem.get_u_i() == 14 # does not crash

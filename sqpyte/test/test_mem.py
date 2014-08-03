from rpython.rtyper.lltypesystem import rffi, lltype
from sqpyte import capi
CConfig = capi.CConfig
from sqpyte.mem import Mem


def test_caching():
    with lltype.scoped_alloc(capi.MEM) as pMem:
        rffi.setintfield(pMem, 'flags', CConfig.MEM_Int)
        pMem.u.i = 17
        pMem.r = 2.3
        mem = Mem(None, pMem, can_cache=True)
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


def test_dont_do_superfluous_writes():
    with lltype.scoped_alloc(capi.MEM) as pMem:
        rffi.setintfield(pMem, 'flags', CConfig.MEM_Int)
        pMem.u.i = 17
        pMem.r = 2.3
        mem = Mem(None, pMem, can_cache=True)
        assert mem.get_flags() == CConfig.MEM_Int
        mem.pMem = None
        mem.set_flags(CConfig.MEM_Int) # works, because unnecessary

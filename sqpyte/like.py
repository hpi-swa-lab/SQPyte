from rpython.rlib import jit, rstring, objectmodel
from rpython.rtyper.lltypesystem import rffi, lltype

from sqpyte.function import CantDoThis

def like_as_sqlite_func(hlquery, args, memout):
    assert len(args) == 2
    pattern_mem, string_mem = args
    z = pattern_mem.get_z()
    pattern = pattern_mem._python_func_cache
    if pattern is None or pattern.llstring != z:
        pystring = rffi.charp2strn(z, pattern_mem.get_n())
        pattern = Pattern(pystring, z)
        pattern_mem._python_func_cache = pattern
    elif not objectmodel.we_are_translated():
        print "reusing",
    if not objectmodel.we_are_translated():
        pystring = rffi.charp2strn(string_mem.get_z(), string_mem.get_n())
        print pystring, pattern.pattern,
    cache_state = hlquery.mem_cache.cache_state()
    hlquery.mem_cache.hide()
    res = pattern.like(string_mem.get_z(), string_mem.get_n())
    hlquery.mem_cache.reveal(cache_state)
    if res:
        memout.sqlite3_result_int64(1, constant=True)
    else:
        memout.sqlite3_result_int64(0, constant=True)
    if not objectmodel.we_are_translated():
        print res



def like(string, pattern):
    pat = Pattern(pattern)
    return pat.like(string)

def get_printable_location(pattern):
    return pattern.pattern

jitdriver = jit.JitDriver(
    greens=["self"],
    reds=["length", "string"],
    get_printable_location=get_printable_location,
    )

class Pattern(object):
    _immutable_fields_ = ["prefix", "middle[*]", "skips_and_masks[*]", "suffix", "pattern", "llstring"]
    def __init__(self, pattern, llstring):
        self.llstring = llstring

        pattern = pattern.lower()
        oldpattern = ''
        while oldpattern != pattern:
            oldpattern = pattern
            pattern = rstring.replace(pattern, "_%", "%")
            pattern = rstring.replace(pattern, "%_", "%")

        parts = pattern.split("%")
        self.prefix = parts[0]
        if len(parts) > 2:
            end = len(parts) - 1
            assert end >= 0
            self.middle = parts[1:end]
        else:
            self.middle = []
        self.skips_and_masks = [compute_skip_and_mask(m) for m in self.middle]
        if len(parts) > 1:
            self.suffix = parts[-1]
        else:
            self.suffix = None
        self.pattern = pattern

    def like(self, string, length):
        jitdriver.jit_merge_point(self=self, string=string, length=length)
        if length < len(self.prefix):
            return False
        #import pdb; pdb.set_trace()
        i = 0
        for c in self.prefix:
            if not char_match_pat_known(string[i], c):
                return False
            i += 1
        jit.jit_debug("after prefix")
        for partindex, nextpart in enumerate(self.middle):
            # of form %nextpart
            if not nextpart:
                continue
            skip, mask = self.skips_and_masks[partindex]
            # search for nextpart
            search = switch_search("_" in nextpart, skip != 0)
            i = search(string, length, nextpart, skip, mask, i, length - len(self.suffix))
            if i == -1:
                return False
            i += len(nextpart)
        # of form %suffix
        jit.jit_debug("after middle")
        if self.suffix is None:
            return i == length
        if i > length - len(self.suffix):
            return False
        i = length - len(self.suffix)
        for c in self.suffix:
            if not char_match_pat_known(string[i], c):
                return False
            i += 1
        return True



from rpython.rlib.rarithmetic import LONG_BIT as BLOOM_WIDTH

def bloom_add(mask, c):
    return mask | (1 << (ord(c) & (BLOOM_WIDTH - 1)))

def bloom(mask, c):
    return mask & (1 << (ord(c) & (BLOOM_WIDTH - 1)))

def char_match(sc, pc):
    return sc.lower() == pc or pc == "_"

def char_match_pat_known(sc, pc):
    if pc.isalpha():
        if ord(sc) >= ord('a'):
            return sc == pc
        else:
            return sc == pc.upper()
    if pc == "_":
        if ord(sc) > 127:
            raise CantDoThis
        return True
    return sc == pc

def compute_skip_and_mask(pattern):
    lenpattern = len(pattern)
    mlast = lenpattern - 1
    skip = mlast - 1
    for i in range(mlast):
        if char_match(pattern[i], pattern[mlast]) or char_match(pattern[mlast], pattern[i]):
            skip = mlast - i - 1
    mask = 0
    for i in range(lenpattern):
        mask = bloom_add(bloom_add(mask, pattern[i]), pattern[i].upper())
        if pattern[i] == "_":
            mask = ~0
    return skip, mask

def make_search(contains_underscore, haveskip, bloom=bloom):
    if contains_underscore:
        cm = char_match
        bloom = lambda a, c: True
    else:
        cm = lambda sc, pc: sc.lower() == pc
    def search(string, lenstring, pattern, skip, mask, start, end):
        count = 0
        n = end - start
        lenpattern = len(pattern)
        assert lenpattern
        mlast = lenpattern - 1

        w = n - lenpattern

        if w < 0:
            return -1

        i = start - 1
        while i + 1 <= start + w:
            i += 1
            if cm(string[i + lenpattern - 1], pattern[lenpattern - 1]):
                for j in range(mlast):
                    if ord(string[i + j]) > 127:
                        raise CantDoThis
                    if not cm(string[i + j], pattern[j]):
                        break
                else:
                    return i

                if i + lenpattern < lenstring:
                    c = string[i + lenpattern]
                else:
                    c = '\0'
                if not bloom(mask, c):
                    i += lenpattern
                elif haveskip:
                    i += skip
            else:
                if i + lenpattern < lenstring:
                    c = string[i + lenpattern]
                else:
                    c = '\0'
                if not bloom(mask, c):
                    i += lenpattern
        return -1
    if contains_underscore:
        search.__name__ += "_underscore"
    if haveskip:
        search.__name__ += "_haveskip"
    return search

search = search_True_True = make_search(True, True)
search_True_False = make_search(True, False)
search_False_True = make_search(False, True)
search_False_False = make_search(False, False)

def switch_search(contains_underscore, haveskip):
    if contains_underscore:
        if haveskip:
            return search_True_True
        else:
            return search_True_False
    else:
        if haveskip:
            return search_False_True
        else:
            return search_False_False


def test_like_constant():
    assert like("medium", "MEDIUM")
    assert not like("mediumaabc", "MEDIUM")
    assert not like("m", "MEDIUM")


def test_like_only_prefix():
    assert like('meDIum', 'MEDIUM%')
    assert like('meDIumarstarfpastas', 'MEDIUM%')
    assert like('mediumarstarfpastas', 'MEDIUM%')
    assert like('MEDIUMarstarfpastas', 'MEDIUM%')
    assert like('MEDIUMarstarfpastas', 'MEDIUM%%%')
    assert like('MEDIUMarstarfpastas', 'MEDIUM%%%%%%')
    assert like('MEDIUMarstarfpastas', 'MEDIUM%%%%%%%%%as')

    assert not like('ameDIum', 'MEDIUM%')
    assert not like('me', 'MEDIUM%')

def test_complex():
    assert like("abaaabababbbbcbcbcbcarststrstde", "ab%bc%de")
    assert not like("afbcfde", "ab%bc%de")
    assert like("abcfdedebcdedededede", "ab%bc%de")

def test_underscore():
    assert like("a", "_")
    assert like("A", "_")
    assert like("5", "_")
    assert not like("5a", "_")

    p = Pattern("_____________________________%____________________")
    assert p.pattern == "%"

    assert like("afbcde", "a_b%")
    assert like("a?b", "a_b%")

    assert like("afbced", "a_b%c_d")
    assert like("foooooooooooooooooa?baaaaaaaaaaaaaar", "%a_b%")

def test_oddballs():
    assert not like("xxab", "xx%ab%ab")


import sys, os
from rpython.rlib import jit
from rpython import conftest
class o:
    view = False
    viewloops = True
conftest.option = o

from rpython.jit.metainterp.test.test_ajit import LLJitMixin


class TestLLtype(LLJitMixin):

    def test_simple(self):

        def interp_w():
            res = 0
            p = Pattern(".._f%a_b%r_..")
            for i in range(100):
                s =  "X" * (i % 11 == 0) + "...f" + "o" * i + "a?b" + "a.A" * i + "r..." + "X" * (i % 3 == 0)
                if i % 7 == 5:
                    s = s.upper()
                if p.like(s):
                    res += 1
            return res
        res = interp_w()
        assert res
        self.meta_interp(interp_w, [], listcomp=True, listops=True, backendopt=True, inline=True)


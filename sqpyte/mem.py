from rpython.rlib import jit, rarithmetic, objectmodel
from rpython.rtyper.lltypesystem import rffi, lltype
from sqpyte.capi import CConfig
from sqpyte import capi
import sys

class Mem(object):
    _immutable_fields_ = ['hlquery', 'pMem', '_cache_index']
    _attrs_ = ['hlquery', 'pMem', '_cache_index', '_cache']

    def __init__(self, hlquery, pMem, _cache_index=-1):
        self.hlquery = hlquery
        self.pMem = pMem
        self._cache_index = _cache_index

    def invalidate_cache(self):
        if self._cache_index != -1:
            self.hlquery.mem_cache.invalidate(self._cache_index)

    def check_cache_consistency(self):
        if self._cache_index == -1:
            return
        state = self.hlquery.mem_cache.cache_state()
        if state.is_flag_known(self._cache_index):
            assert rffi.cast(lltype.Unsigned, self.pMem.flags) == self.get_flags()
        if state.is_r_known(self._cache_index):
            assert self.pMem.r == self.get_r()
        if state.is_u_i_known(self._cache_index):
            assert self.pMem.u.i == self.get_u_i()

    def get_flags(self, promote=False):
        flags = self.hlquery.mem_cache.get_flags(self)
        if promote:
            jit.promote(flags)
        return flags

    def set_flags(self, newflags):
        self.hlquery.mem_cache.set_flags(self, newflags)

    def get_r(self):
        return self.hlquery.mem_cache.get_r(self)

    def set_r(self, val):
        return self.hlquery.mem_cache.set_r(self, val)


    def get_u_i(self):
        return self.hlquery.mem_cache.get_u_i(self)

    def set_u_i(self, val, constant=False):
        return self.hlquery.mem_cache.set_u_i(self, val, constant)

    def is_constant_u_i(self):
        return self.hlquery.mem_cache.is_constant_u_i(self)


    def get_n(self):
        return rffi.cast(lltype.Signed, self.pMem.n)

    def set_n(self, val):
        rffi.setintfield(self.pMem, 'n', val)

    def get_enc(self):
        return self.pMem.enc


    def get_z(self):
        return self.pMem.z

    def set_z(self, val):
        self.pMem.z = val

    def set_z_null(self):
        self.set_z(lltype.nullptr(rffi.CCHARP.TO))


    def get_zMalloc(self):
        return self.pMem.zMalloc

    def set_zMalloc(self, val):
        self.pMem.zMalloc = val

    def set_zMalloc_null(self):
        self.set_zMalloc(lltype.nullptr(lltype.typeOf(self.pMem).TO.zMalloc.TO))


    def get_db(self):
        return self.pMem.db

    def set_db(self, db):
        self.pMem.db = db


    def get_xDel(self):
        return self.pMem.xDel

    def set_xDel(self, val):
        self.pMem.xDel = val

    def set_xDel_null(self):
        self.set_xDel(lltype.nullptr(lltype.typeOf(self.pMem).TO.xDel.TO))

    # _______________________________________________________________
    # methods induced by sqlite3 functions below

    def sqlite3VdbeIntegerAffinity(self):
        """
        The MEM structure is already a MEM_Real.  Try to also make it a
        MEM_Int if we can.
        """
        self._sqlite3VdbeIntegerAffinity_flags(self.get_flags())

    def _sqlite3VdbeIntegerAffinity_flags(self, flags):
        assert flags & CConfig.MEM_Real
        assert not flags & CConfig.MEM_RowSet
        # assert( mem->db==0 || sqlite3_mutex_held(mem->db->mutex) );
        # assert( EIGHT_BYTE_ALIGNMENT(mem) );
        floatval = self.get_r()
        intval = int(floatval)
        # Only mark the value as an integer if
        #
        #    (1) the round-trip conversion real->int->real is a no-op, and
        #    (2) The integer is neither the largest nor the smallest
        #        possible integer (ticket #3922)
        #
        # The second and third terms in the following conditional enforces
        # the second condition under the assumption that addition overflow causes
        # values to wrap around.
        if floatval == float(intval) and intval < sys.maxint and intval > (-sys.maxint - 1):
            flags = flags | CConfig.MEM_Int
            self.set_flags(flags)
        return flags

    def applyAffinity(self, affinity, enc):
        """
         Processing is determine by the affinity parameter:

         SQLITE_AFF_INTEGER:
         SQLITE_AFF_REAL:
         SQLITE_AFF_NUMERIC:
            Try to convert pRec to an integer representation or a
            floating-point representation if an integer representation
            is not possible.  Note that the integer representation is
            always preferred, even if the affinity is REAL, because
            an integer representation is more space efficient on disk.

         SQLITE_AFF_TEXT:
            Convert pRec to a text representation.

         SQLITE_AFF_NONE:
            No-op.  pRec is unchanged.
        """
        flags = self.get_flags()

        return self._applyAffinity_flags_read(flags, affinity, enc)

    def _applyAffinity_flags_read(self, flags, affinity, enc):
        assert isinstance(affinity, int)
        if affinity == CConfig.SQLITE_AFF_TEXT:
            # Only attempt the conversion to TEXT if there is an integer or real
            # representation (blob and NULL do not get converted) but no string
            # representation.

            if not (flags & CConfig.MEM_Str) and flags & (CConfig.MEM_Real|CConfig.MEM_Int):
                self.invalidate_cache()
                capi.sqlite3_sqlite3VdbeMemStringify(self.pMem, enc)
                flags = self.get_flags()
            flags = flags & ~(CConfig.MEM_Real|CConfig.MEM_Int)
            self.set_flags(flags)
        elif affinity != CConfig.SQLITE_AFF_NONE:
            assert affinity in (CConfig.SQLITE_AFF_INTEGER,
                                CConfig.SQLITE_AFF_REAL,
                                CConfig.SQLITE_AFF_NUMERIC)
            flags = self._applyNumericAffinity_flags_read(flags)
            if flags & CConfig.MEM_Real:
                flags = self._sqlite3VdbeIntegerAffinity_flags(flags)
        return flags

    def sqlite3VdbeMemIntegerify(self):
        self.invalidate_cache()
        return capi.sqlite3_sqlite3VdbeMemIntegerify(self)

    def applyNumericAffinity(self):
        """
        Try to convert a value into a numeric representation if we can
        do so without loss of information.  In other words, if the string
        looks like a number, convert it into a number.  If it does not
        look like a number, leave it alone.
        """
        _applyNumericAffinity_flags_read(self, self.get_flags())

    def _applyNumericAffinity_flags_read(self, flags):
        if flags & (CConfig.MEM_Real|CConfig.MEM_Int):
            return flags
        if not flags & CConfig.MEM_Str:
            return flags
        # use the C function as a slow path for now
        self.invalidate_cache()
        capi.sqlite3_applyNumericAffinity(self.pMem)
        return self.get_flags()

    def numericType(self):
        return self._numericType_with_flags(self.get_flags())

    def _numericType_with_flags(self, flags):
        if flags & (CConfig.MEM_Int | CConfig.MEM_Real):
            return flags & (CConfig.MEM_Int | CConfig.MEM_Real)
        if flags & (CConfig.MEM_Str | CConfig.MEM_Blob):
            val1 = lltype.malloc(rffi.DOUBLEP.TO, 1, flavor='raw')
            val1[0] = 0.0
            atof = capi.sqlite3AtoF(self.get_z(), val1, self.get_n(), self.get_enc())
            self.set_r(val1[0])
            lltype.free(val1, flavor='raw')

            if atof == 0:
                return 0

            val2 = lltype.malloc(rffi.LONGLONGP.TO, 1, flavor='raw')
            val2[0] = 0
            atoi64 = capi.sqlite3Atoi64(self.get_z(), val2, self.get_n(), self.get_enc())
            self.set_u_i(val2[0])
            lltype.free(val2, flavor='raw')

            if atoi64 == CConfig.SQLITE_OK:
                return CConfig.MEM_Int

            return CConfig.MEM_Real
        return 0

    def sqlite3VdbeMemRelease(self):
        self.VdbeMemRelease()
        if self.get_zMalloc():
            capi.sqlite3DbFree(self.hlquery.db, rffi.cast(rffi.VOIDP, self.get_zMalloc()))
            self.set_zMalloc(lltype.nullptr(rffi.CCHARP.TO))
        self.set_z(lltype.nullptr(rffi.CCHARP.TO))

    def sqlite3VdbeMemSetInt64(self, val):
        self.sqlite3VdbeMemRelease()
        self.set_u_i(val)
        self.set_flags(CConfig.MEM_Int)

    def sqlite3VdbeMemSetNull(self):
        """ Delete any previous value and set the value stored in *pMem to NULL. """
        if self.get_flags() & CConfig.MEM_Frame or self.get_flags() & CConfig.MEM_RowSet:
            self.invalidate_cache()
            capi.sqlite3VdbeMemSetNull(self.pMem)
        else:
            # fast path
            self.MemSetTypeFlag(CConfig.MEM_Null)

    # Make an shallow copy of pFrom into pTo.  Prior contents of
    # pTo are freed.  The pFrom->z field is not duplicated.  If
    # pFrom->z is used, then pTo->z points to the same thing as pFrom->z
    # and flags gets srcType (either MEM_Ephem or MEM_Static).
    # def sqlite3VdbeMemShallowCopy(self, pTo, pFrom, srcType):
    #     assert (pFrom.flags & CConfig.MEM_RowSet) == 0
    #     pTo.VdbeMemRelease()
    #     pTo = copy.deepcopy(pFrom)
    #     # memcpy(pTo, pFrom, MEMCELLSIZE);
    #     pTo.xDel = 0
    #     if (pFrom.flags & CConfig.MEM_Static) == 0:
    #         pTo.flags &= ~(CConfig.MEM_Dyn | CConfig.MEM_Static | CConfig.MEM_Ephem)
    #         assert srcType == CConfig.MEM_Ephem or srcType == CConfig.MEM_Static
    #         pTo.flags |= srcType

    def MemSetTypeFlag(self, flags):
        self.set_flags((self.get_flags(promote=True) & ~(CConfig.MEM_TypeMask | CConfig.MEM_Zero)) | flags)


    def _MemSetTypeFlag_flags(self, oldflags, flags):
        newflags = (oldflags & ~(CConfig.MEM_TypeMask | CConfig.MEM_Zero)) | flags
        if newflags != oldflags:
            self.set_flags(newflags)

    def VdbeMemDynamic(self):
        return (self.get_flags() & (CConfig.MEM_Agg|CConfig.MEM_Dyn|CConfig.MEM_RowSet|CConfig.MEM_Frame))!=0

    def VdbeMemRelease(self):
        if self.VdbeMemDynamic():
            self.invalidate_cache()
            capi.sqlite3VdbeMemReleaseExternal(self.pMem)


    def sqlite3VdbeIntValue(self):
        """
        Return some kind of integer value which is the best we can do
        at representing the value that *pMem describes as an integer.
        If pMem is an integer, then the value is exact.  If pMem is
        a floating-point then the value returned is the integer part.
        If pMem is a string or blob, then we make an attempt to convert
        it into a integer and return that.  If pMem represents an
        an SQL-NULL value, return 0.

        If pMem represents a string value, its encoding might be changed.
        """

        return self._sqlite3VdbeIntValue_flags(self.get_flags())

    def _sqlite3VdbeIntValue_flags(self, flags):
        #assert( pMem->db==0 || sqlite3_mutex_held(pMem->db->mutex) );
        #assert( EIGHT_BYTE_ALIGNMENT(pMem) );
        if flags & CConfig.MEM_Int:
            return self.get_u_i()
        elif flags & CConfig.MEM_Real:
            return int(self.get_r())
        elif flags & (CConfig.MEM_Str|CConfig.MEM_Blob):
            val2 = lltype.malloc(rffi.LONGLONGP.TO, 1, flavor='raw')
            val2[0] = 0
            capi.sqlite3Atoi64(self.get_z(), val2, self.get_n(), self.get_enc())
            value = val2[0]
            lltype.free(val2, flavor='raw')
            return value
        else:
            return 0

    def sqlite3VdbeRealValue(self):
        """
        Return the best representation of pMem that we can get into a
        double.  If pMem is already a double or an integer, return its
        value.  If it is a string or blob, try to convert it to a double.
        If it is a NULL, return 0.0.
        """

        return self._sqlite3VdbeRealValue_flags(self.get_flags())

    def _sqlite3VdbeRealValue_flags(self, flags):
        # assert( pMem->db==0 || sqlite3_mutex_held(pMem->db->mutex) );
        # assert( EIGHT_BYTE_ALIGNMENT(pMem) );
        if flags & CConfig.MEM_Real:
            return self.get_r()
        elif flags & CConfig.MEM_Int:
            return self.get_u_i()
        elif flags & (CConfig.MEM_Str | CConfig.MEM_Blob):
            val = lltype.malloc(rffi.DOUBLEP.TO, 1, flavor='raw')
            val[0] = 0.0
            capi.sqlite3AtoF(self.get_z(), val, self.get_n(), self.get_enc())
            ret = val[0]
            lltype.free(val, flavor='raw')
            return ret
        else:
            return 0.0

    # _______________________________________________________________
    # methods induced by sqlite3 functions below


    def sqlite3MemCompare(self, other, coll):
        flags1 = self.get_flags()
        flags2 = other.get_flags()
        combined_flags = flags1 | flags2
        if combined_flags & CConfig.MEM_Null:
            return rarithmetic.intmask(flags2 & CConfig.MEM_Null) - rarithmetic.intmask(flags1 & CConfig.MEM_Null)

        # If one value is a number and the other is not, the number is less.
        # If both are numbers, compare as reals if one is a real, or as integers
        # if both values are integers.
        if (flags1 | flags2) & (CConfig.MEM_Int | CConfig.MEM_Real):
            # both are ints
            if flags1 & flags2 & CConfig.MEM_Int:
                i1 = self.get_u_i()
                i2 = other.get_u_i()
                if i1 < i2:
                    return -1
                if i1 > i2:
                    return 1
                return 0
            else:
                if flags1 & CConfig.MEM_Real:
                    r1 = self.get_r()
                elif flags1 & CConfig.MEM_Int:
                    r1 = float(self.get_u_i())
                else:
                    return 1
                if flags2 & CConfig.MEM_Real:
                    r2 = other.get_r()
                elif flags2 & CConfig.MEM_Int:
                    r2 = float(other.get_u_i())
                else:
                    return 1
                if r1 < r2:
                    return -1
                if r1 > r2:
                    return 1
                return 0
        self.invalidate_cache()
        other.invalidate_cache()
        return capi.sqlite3_sqlite3MemCompare(self.pMem, other.pMem, coll)




class CacheHolder(object):
    _immutable_fields_ = ['integers', 'floats']

    def __init__(self, num_flags):
        self._cache_state = all_unknown(num_flags)
        self.integers = [0] * num_flags
        self.floats = [0.0] * num_flags

    def cache_state(self):
        return jit.promote(self._cache_state)

    def hide(self):
        pass

    def reveal(self, x):
        pass

    def invalidate(self, i):
        self._cache_state = self.cache_state().set_unknown(i)

    def invalidate_all(self):
        self._cache_state = self._invalid_cache_state

    def get_flags(self, mem):
        i = mem._cache_index
        if i == -1:
            return rffi.cast(lltype.Unsigned, mem.pMem.flags)
        state = self.cache_state()
        if state.is_flag_known(i):
            if not objectmodel.we_are_translated() and mem.pMem:
                assert state.get_flags(i) == rffi.cast(lltype.Unsigned, mem.pMem.flags)
            return state.get_flags(i)
        flags = rffi.cast(lltype.Unsigned, mem.pMem.flags)
        self._cache_state = state.change_flags(i, flags)
        return flags

    def set_flags(self, mem, newflags):
        i = mem._cache_index
        if i == -1:
            rffi.setintfield(mem.pMem, 'flags', newflags)
            return
        state = self.cache_state()
        if state.is_flag_known(i) and state.get_flags(i) == newflags:
            return
        rffi.setintfield(mem.pMem, 'flags', newflags)
        self._cache_state = state.change_flags(i, newflags)
        if not objectmodel.we_are_translated() and mem.pMem:
            assert self._cache_state.get_flags(i) == rffi.cast(lltype.Unsigned, mem.pMem.flags) == newflags

    def get_r(self, mem):
        i = mem._cache_index
        if i == -1:
            return mem.pMem.r
        state = self.cache_state()
        if state.is_r_known(i):
            if not objectmodel.we_are_translated() and mem.pMem:
                assert self.floats[i] == mem.pMem.r
            return self.floats[i]
        r = mem.pMem.r
        self.floats[i] = r
        self._cache_state = state.add_knowledge(i, STATE_FLOAT_KNOWN)
        return r

    def set_r(self, mem, r):
        i = mem._cache_index
        if i == -1:
            mem.pMem.r = r
            return
        state = self.cache_state()
        mem.pMem.r = r
        self.floats[i] = r
        self._cache_state = state.add_knowledge(i, STATE_FLOAT_KNOWN)

    def get_u_i(self, mem):
        i = mem._cache_index
        if i == -1:
            return mem.pMem.u.i
        state = self.cache_state()
        if state.is_constant_u_i(i):
            return state.get_constant_u_i(i)
        if state.is_u_i_known(i):
            return self.integers[i]
        u_i = mem.pMem.u.i
        self.integers[i] = u_i
        self._cache_state = state.add_knowledge(i, STATE_INT_KNOWN)
        return u_i

    def set_u_i(self, mem, u_i, constant=False):
        i = mem._cache_index
        if i == -1:
            mem.pMem.u.i = u_i
            return
        state = self.cache_state()
        mem.pMem.u.i = u_i
        if not constant:
            self.integers[i] = u_i
            status = (state.cache_states[i] & ~STATE_CONSTANT) | STATE_INT_KNOWN
            self._cache_state = state.change_cache_state(i, status)
        else:
            self._cache_state = state.set_u_i_constant(i, u_i)

    def is_constant_u_i(self, mem):
        i = mem._cache_index
        if i == -1:
            return False
        return self.cache_state().is_constant_u_i(i)


STATE_UNKNOWN = 0
STATE_FLAG_KNOWN = 1
STATE_INT_KNOWN = 2
STATE_FLOAT_KNOWN = 4
STATE_CONSTANT = 8

def state_eq(self, other):
    return self.eq(other)

def state_hash(self):
    return self.hash()

class CacheState(object):
    _immutable_fields_ = ['all_flags[*]', 'cache_states[*]', 'u_i_constants[*]']
    _cache = objectmodel.r_dict(state_eq, state_hash)

    def __init__(self, all_flags, cache_states, u_i_constants):
        self.all_flags = all_flags
        self.cache_states = cache_states
        self.u_i_constants = u_i_constants

    def copy(self):
        return CacheState(self.all_flags[:], self.cache_states[:], self.u_i_constants[:])

    def repr(self):
        return "CacheState(%s, %s, %s)" % (self.all_flags, self.cache_states, self.u_i_constants)
    __repr__ = repr

    def eq(self, other):
        return (self.all_flags == other.all_flags and
                self.cache_states == other.cache_states and
                self.u_i_constants == other.u_i_constants
                )


    def hash(self):
        from rpython.rlib.rarithmetic import intmask
        x = 0x345678
        for item in self.all_flags:
            y = rffi.cast(lltype.Signed, item)
            x = intmask((1000003 * x) ^ y)
        for item in self.cache_states:
            y = rffi.cast(lltype.Signed, item)
            x = intmask((1000003 * x) ^ y)
        for item in self.u_i_constants:
            y = rffi.cast(lltype.Signed, item)
            x = intmask((1000003 * x) ^ y)
        return x

    def unique(self):
        if self in self._cache:
            newself = self._cache[self]
            assert newself.all_flags == self.all_flags
            assert newself.cache_states == self.cache_states
            assert newself.u_i_constants == self.u_i_constants
            return newself
        self._cache[self] = self
        return self

    @jit.elidable_promote('all')
    def change_flags(self, i, new_flags):
        if self.is_flag_known(i) and self.all_flags[i] == new_flags:
            return self
        self = self.add_knowledge(i, STATE_FLAG_KNOWN)
        result = self.copy()
        result.all_flags[i] = new_flags
        return result.unique()

    def add_knowledge(self, i, statusbits):
        status = self.cache_states[i] | statusbits
        return self.change_cache_state(i, status)

    @jit.elidable_promote('all')
    def change_cache_state(self, i, status):
        if self.cache_states[i] == status:
            return self
        result = self.copy()
        result.cache_states[i] = status
        return result.unique()

    @jit.elidable_promote('all')
    def set_u_i_constant(self, i, u_i):
        self = self.add_knowledge(i, STATE_INT_KNOWN | STATE_CONSTANT)
        result = self.copy()
        result.u_i_constants[i] = u_i
        return result.unique()

    def set_unknown(self, i):
        return self.change_flags(i, 0).change_cache_state(i, STATE_UNKNOWN)

    def is_flag_known(self, i):
        return bool(self.cache_states[i] & STATE_FLAG_KNOWN)

    def is_r_known(self, i):
        return bool(self.cache_states[i] & STATE_FLOAT_KNOWN)

    def is_u_i_known(self, i):
        return bool(self.cache_states[i] & STATE_INT_KNOWN)

    def is_constant_u_i(self, i):
        return self.is_u_i_known(i) and bool(self.cache_states[i] & STATE_CONSTANT)

    def get_flags(self, i):
        assert self.is_flag_known(i)
        return self.all_flags[i]

    def get_constant_u_i(self, i):
        assert self.is_constant_u_i(i)
        return self.u_i_constants[i]

def all_unknown(num_flags):
    return CacheState([0] * num_flags, [0] * num_flags, [0] * num_flags)

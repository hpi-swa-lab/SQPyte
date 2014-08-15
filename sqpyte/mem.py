from rpython.rlib import jit, rarithmetic
from rpython.rtyper.lltypesystem import rffi
from sqpyte.capi import CConfig
from sqpyte import capi
from rpython.rtyper.lltypesystem import lltype
import sys

class Mem(object):
    _immutable_fields_ = ['hlquery', 'pMem']
    _attrs_ = ['hlquery', 'pMem']

    def __init__(self, hlquery, pMem):
        self.hlquery = hlquery
        self.pMem = pMem

    def get_flags(self, promote=False):
        flags = rffi.cast(lltype.Unsigned, self.pMem.flags)
        if promote:
            jit.promote(flags)
        return flags

    def set_flags(self, newflags):
        self.pMem.flags = rffi.cast(CConfig.u16, newflags)

    def get_r(self):
        return self.pMem.r

    def set_r(self, val):
        self.pMem.r = val


    def get_u_i(self):
        return self.pMem.u.i

    def set_u_i(self, val):
        self.pMem.u.i = val


    def get_u_nZero(self):
        return self.pMem.u.nZero

    def set_u_nZero(self, val):
        self.pMem.u.nZero = val


    def get_n(self):
        return rffi.cast(lltype.Signed, self.pMem.n)

    def set_n(self, val):
        rffi.setintfield(self.pMem, 'n', val)

    def get_enc(self):
        return self.pMem.enc

    def set_enc(self, val):
        self.pMem.enc = val

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
        capi.sqlite3VdbeMemSetNull(self.pMem)

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
        self.set_flags((self.get_flags() & ~(CConfig.MEM_TypeMask | CConfig.MEM_Zero)) | flags)


    def _MemSetTypeFlag_flags(self, oldflags, flags):
        newflags = (oldflags & ~(CConfig.MEM_TypeMask | CConfig.MEM_Zero)) | flags
        if newflags != oldflags:
            self.set_flags(newflags)

    def VdbeMemDynamic(self):
        return (self.get_flags() & (CConfig.MEM_Agg|CConfig.MEM_Dyn|CConfig.MEM_RowSet|CConfig.MEM_Frame))!=0

    def VdbeMemRelease(self):
        if self.VdbeMemDynamic():
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


    def sqlite3VdbeMemShallowCopy(self, other, srcType):
        """
        Make an shallow copy of pFrom into pTo.  Prior contents of
        pTo are freed.  The pFrom->z field is not duplicated.  If
        pFrom->z is used, then pTo->z points to the same thing as pFrom->z
        and flags gets srcType (either MEM_Ephem or MEM_Static).
        """
        MEMCELLSIZE = rffi.offsetof(capi.MEM, 'zMalloc')
        assert not other.get_flags() & CConfig.MEM_RowSet
        self.VdbeMemRelease()
        rffi.c_memcpy(rffi.cast(rffi.VOIDP, self.pMem), rffi.cast(rffi.VOIDP, other.pMem), MEMCELLSIZE)
        self.set_xDel_null()
        if not other.get_flags() & CConfig.MEM_Static:
            flags = self.get_flags()
            flags &= ~(CConfig.MEM_Dyn | CConfig.MEM_Static | CConfig.MEM_Ephem)
            assert srcType == CConfig.MEM_Ephem or srcType == CConfig.MEM_Static
            self.set_flags(flags | srcType)

    def sqlite3MemCompare(self, other, coll):
        return capi.sqlite3_sqlite3MemCompare(self.pMem, other.pMem, coll)

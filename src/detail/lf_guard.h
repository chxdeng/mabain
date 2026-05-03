/**
 * Internal helper: lock-free reader guard used in lookup paths.
 * Not part of the public API.
 */
#pragma once

#include "error.h"
#include "lock_free.h"
#include "mb_data.h"

namespace mabain {

struct ReaderLFGuard {
#ifdef __LOCK_FREE__
    LockFree& lf;
    MBData& data;
    LockFreeData snap;
    explicit ReaderLFGuard(LockFree& l, MBData& d)
        : lf(l)
        , data(d)
    {
        lf.ReaderLockFreeStart(snap);
    }
    inline int stop(size_t edge_off) { return lf.ReaderLockFreeStop(snap, edge_off, data); }
#else
    explicit ReaderLFGuard(LockFree&, MBData&) { }
    inline int stop(size_t) { return MBError::SUCCESS; }
#endif
    inline int stopOrReturn(size_t edge_off, int result)
    {
        int lf_result = stop(edge_off);
        return lf_result == MBError::SUCCESS ? result : lf_result;
    }
};

} // namespace mabain

/**
 * Internal helper: lock-free reader guard used in find paths.
 * Not part of the public API.
 */
#pragma once

#ifdef __LOCK_FREE__
#include "lock_free.h"
#include "mb_data.h"

namespace mabain {

struct ReaderLFGuard {
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
};

} // namespace mabain

#endif // __LOCK_FREE__

#!/usr/bin/env python

from pathlib import Path
import os, platform

from cffi import FFI

CWD = Path(__file__).parent.resolve()
PREFIX = os.environ.get('PREFIX', os.environ.get('CONDA_PREFIX'))
SHARED_LIB_SUFFIX = '.dylib' if platform.system() == 'Darwin' else ".so"
STATIC_LIB_SUFFIX = ".a"

ffi = FFI()
with open(CWD / 'include/cdef.h') as f:
    cdef = []
    exclude = [
        'createLwsIotCredentialProvider',
        'createLwsIotCredentialProviderWithTime',
    ]
    skipping = False
    struct = False
    for line in f.read().splitlines():
        if skipping:
            if line[-1] == ';':
                skipping = False
            print(f"skip: {line}")
            continue
        if not line.strip():
            continue
        if '<<' in line:
            lhs, rhs = line.split('=')
            res = eval(rhs)
            if isinstance(res, tuple):
                res = res[0]
            line = f"{lhs} = {res},"
            print('eval: ', line)

        if '= (BYTE)' in line:
            line = ''.join(line.split('(BYTE)'))
            print('filtered:', line)

        if '= (UINT16)' in line:
            line = ''.join(line.split('(UINT16)'))
            print('filtered:', line)

        if any(map(lambda fn: fn in line, exclude)):
            if line[-1] != ';':
                skipping = True
            print(f"skip: {line}")
            continue

        if 'struct' in line and line.endswith('{'):
            struct = True

        if struct and line.startswith('}'):
            cdef.append('\t...;')
            struct = False

        cdef.append(line)
    cdef = '\n'.join(cdef)

with open(CWD / 'include/constants.h') as f:
    constants = []
    for line in f.read().splitlines():
        tokens = line.split()
        if len(tokens) == 3 and tokens[-1].isnumeric():
            constants.append(line)
    constants = '\n'.join(constants)
    print(constants)

cdef = f"""
typedef signed char int8_t;
typedef short int int16_t;
typedef int int32_t;
typedef long int int64_t;
typedef unsigned char uint8_t;
typedef unsigned short int uint16_t;
typedef unsigned int uint32_t;
typedef unsigned long int uint64_t;
typedef signed char int_least8_t;
typedef short int int_least16_t;
typedef int int_least32_t;
typedef long int int_least64_t;
typedef unsigned char uint_least8_t;
typedef unsigned short int uint_least16_t;
typedef unsigned int uint_least32_t;
typedef unsigned long int uint_least64_t;
typedef signed char int_fast8_t;
typedef long int int_fast16_t;
typedef long int int_fast32_t;
typedef long int int_fast64_t;
typedef unsigned char uint_fast8_t;
typedef unsigned long int uint_fast16_t;
typedef unsigned long int uint_fast32_t;
typedef unsigned long int uint_fast64_t;
typedef long int intptr_t;
typedef unsigned long int uintptr_t;
typedef long int intmax_t;
typedef unsigned long int uintmax_t;

typedef ... pthread_cond_t;

{cdef}
typedef UINT32 STATUS;
void * memcpy(void *to, const void *from, size_t numBytes);

{constants}
const UINT64 MAX_UINT64 = 0xffffffffffffffff;
const STATUS STATUS_SUCCESS = 0x00000000;
const UINT64 INVALID_STREAM_HANDLE_VALUE = ((STREAM_HANDLE) INVALID_HANDLE_VALUE);
const UINT64 INVALID_CLIENT_HANDLE_VALUE = ((CLIENT_HANDLE) INVALID_HANDLE_VALUE);
"""

source = f"""
#include <com/amazonaws/kinesis/video/cproducer/Include.h>
"""

'''
with open('cffi-cdef.h', 'w') as f:
    f.write(cdef)

with open('cffi-source.c', 'w') as f:
    f.write(source)
'''

ffi.cdef(cdef, pack=1, override=True)
ffi.set_source("ml.streaming._C", source,
    include_dirs=[f"{PREFIX}/include"],
    library_dirs=[f"{PREFIX}/lib"],
    libraries=['cproducer', 'kvsCommonCurl', 'kvspic', 'kvspicClient', 'kvspicState', 'kvspicUtils', 'curl', 'crypto'],
)

if __name__ == '__main__':
    ffi.compile(verbose=True)
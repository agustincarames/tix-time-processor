#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

# Converted to Python by Paula Verghelet 2013
# Converted to R by Richard G. Clegg (richard@richardclegg.org) 2005
# Written by Bob Sherman, modified by Walter Willinger, Vadim Teverovsky.

import math
import numpy
from . import crs


def plotrs(data, debug):
    nblk = 5
    nlag = 50
    power1 = 0.7
    power2 = 2.5
    shuffle = 0
    overlap = 1
    ndiff = 0
    lag = 0
    rflag = 1
    slopes = 1
    fitls = 1
    connect_ = 0
    if debug == 1:
        print(data, nblk, nlag, power1, power2, shuffle, overlap, ndiff, lag, rflag, slopes, fitls, '\n\n')
    ret = "OK"
    n = len(data)
    if overlap != 0:
        increment = math.log10(n) / nlag
    else:
        increment = math.log10(blksize) / nlag
    if debug == 1:
        print("DATA", data, "\n\n")
    output = [0 for x in range(0, (2 * nblk * nlag))]
    range_ = []
    crs.Crs(data, len(data), nblk, nlag, overlap, output)
    range_ = output
    if debug == 3:
        print("\n\nRANGE ", str(range_))
    x = []
    r = []
    ra = []
    xc = []
    rc = []
    rac = []
    for i in range(0, nlag):
        if i * increment < power1:
            xc += [math.log10(math.floor(math.pow(10, (i * increment))))] * nblk
            # Above line changed 2/28/95 to make the plotting consistent
            # with calculations.
            # range_[desde:hasta]
            # desde 0 o desde 1?
            rc += range_[((i - 1) * nblk):(i * nblk)]
            rac += range_[(nblk * nlag + (i - 1) * nblk):(nblk * nlag + i * nblk)]
        if (i * increment >= power1) & (math.log10(math.floor(math.pow(10, (i * increment)))) <= power2):
            # Above/below line changed 2/28/95 to make plotting consistent
            # with calculations.
            x += [math.log10(math.floor(math.pow(10, (i * increment))))] * nblk
            r += range_[((i - 1) * nblk):(i * nblk)]
            ra += range_[(nblk * nlag + (i - 1) * nblk):(nblk * nlag + i * nblk)]
            if debug == 1:
                print("X", x, "\n")
                print("R", r, "\n")
                print("RA", ra, "\n")
        if i * increment > power2:
            xc += [math.log10(math.floor(math.pow(10, (i * increment))))] * nblk
            # Above line changed 2/28/95 to make the plotting consistent
            # with calculations.
            # range_[desde:hasta]
            # desde 0 o desde 1?
            rc += range_[((i - 1) * nblk):(i * nblk)]
            rac += range_[(nblk * nlag + (i - 1) * nblk):(nblk * nlag + i * nblk)]
    if debug == 2:
        print("i", i, "\n")
        print("X", x, "\n")
        print("R", r, "\n")
    if len(list(filter((lambda x1: x1 > 0.0000000001), r))) > 0:
        ld = [x[i] for i in range(0, len(x)) if i in [j for j in range(0, len(r)) if r[j] > 0.0]]
        # ld contiene los valores de x cuya posicion en el vector
        # coincide con la posicion de los valores en r que cumplen la condicion
        rt = filter((lambda x1: x1 > 0), r)
        rat = [ra[i] for i in range(0, len(ra)) if i in [j for j in range(0, len(r)) if r[j] > 0.0]]
        if debug == 1:
            print("RAT", rat)
        lr = map(math.log10, rt)
        lra = map(math.log10, rat)
        if debug == 2:
            print("LD", ld, "\n")
            print("LRA", lra, "\n")
    else:
        # cat("\n either the series is constant or no data was entered.\n\n")
        print("\n either the series is constant or no data was entered.\n\n")
        ret = 0
    if len(list(filter((lambda x1: x1 > 0.0000000001), rc))) > 0:
        ldc = [xc[i] for i in range(0, len(xc)) if i in [j for j in range(0, len(rc)) if rc[j] > 0.0]]
        rtc = filter((lambda x1: x1 > 0), rc)
        ratc = [rac[i] for i in range(0, len(rac)) if i in [j for j in range(0, len(rc)) if rc[j] > 0.0]]
        lrc = map(math.log10, rtc)
        lrac = []
        for i in range(0, len(ratc)):
            if ratc[i] > 0:
                lrac.append(math.log10(ratc[i]))
    else:
        print("\n either the series is constant or no data was entered.\n\n")
        ret = 0
    if rflag != 0:
        # Do the calculations for fitting a least-squares line. For R/S.
        if fitls == 1:
            A = numpy.vstack([ld, numpy.ones(len(ld))]).T
            ba, ma = numpy.linalg.lstsq(A, lra)[0]
            if debug == 1:
                print("LD", ld)
                print("LRA", lra)
            if slopes != 0:
                if rflag != 0:
                    if connect_ == 0:
                        result = ba
                        ret = result
    return ret

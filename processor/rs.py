#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

# Converted to Python by Paula Verghelet 2013
# Converted to R by Richard G. Clegg (richard@richardclegg.org) 2005
# Written by Bob Sherman, modified by Walter Willinger, Vadim Teverovsky.

import math

import logging
import numpy
from . import crs

NBLK = 5
NLAG = 50
POWER1 = 0.7
POWER2 = 2.5
SHUFFEL = 0
OVERLAP = 1
NDIFF = 0
LAG = 0
CONNECT_ = 0


def plotrs(data):
    logger = logging.getLogger('plotrs')
    logger.debug("data: {data}".format(data=data))
    n = len(data)
    increment = math.log10(n) / NLAG
    output = [0] * (2 * NBLK * NLAG)
    crs.Crs(data, len(data), NBLK, NLAG, OVERLAP, output)
    range_ = output
    logger.debug("range: {range}".format(range=str(range_)))
    x = []
    r = []
    ra = []
    xc = []
    rc = []
    rac = []
    for i in range(0, NLAG):
        if i * increment < POWER1:
            xc += [math.log10(math.floor(math.pow(10, (i * increment))))] * NBLK
            # Above line changed 2/28/95 to make the plotting consistent
            # with calculations.
            # range_[desde:hasta]
            # desde 0 o desde 1?
            rc += range_[((i - 1) * NBLK):(i * NBLK)]
            rac += range_[(NBLK * NLAG + (i - 1) * NBLK):(NBLK * NLAG + i * NBLK)]
        if (i * increment >= POWER1) and (math.log10(math.floor(math.pow(10, (i * increment)))) <= POWER2):
            # Above/below line changed 2/28/95 to make plotting consistent
            # with calculations.
            x += [math.log10(math.floor(math.pow(10, (i * increment))))] * NBLK
            r += range_[((i - 1) * NBLK):(i * NBLK)]
            ra += range_[(NBLK * NLAG + (i - 1) * NBLK):(NBLK * NLAG + i * NBLK)]
            logger.debug("x: {x}, r: {r}, ra: {ra}".format(x=x, r=r, ra=ra))
        if i * increment > POWER2:
            xc += [math.log10(math.floor(math.pow(10, (i * increment))))] * NBLK
            # Above line changed 2/28/95 to make the plotting consistent
            # with calculations.
            # range_[desde:hasta]
            # desde 0 o desde 1?
            rc += range_[((i - 1) * NBLK):(i * NBLK)]
            rac += range_[(NBLK * NLAG + (i - 1) * NBLK):(NBLK * NLAG + i * NBLK)]
        logger.debug("i: {i}, x: {x}, ra: {ra}".format(i=i, x=x, ra=ra))
    if len(list(filter((lambda x1: x1 > 0.0000000001), r))) > 0:
        ld = [x[i] for i in range(0, len(x)) if i in [j for j in range(0, len(r)) if r[j] > 0.0]]
        # ld contains the values of x which position in the array coincides with the position of the values in r
        # that satisfies the condition
        rat = [ra[i] for i in range(0, len(ra)) if i in [j for j in range(0, len(r)) if r[j] > 0.0]]
        logger.debug("rat: {rat}".format(rat=rat))
        lra = list(map(math.log10, rat))
        logger.debug("ld: {ld} lra: {lra}".format(ld=ld, lra=lra))
    else:
        raise ValueError("Either the series is constant or no data was entered.")
    if len(list(filter((lambda x1: x1 > 0.0000000001), rc))) > 0:
        ratc = [rac[i] for i in range(0, len(rac)) if i in [j for j in range(0, len(rc)) if rc[j] > 0.0]]
        lrac = []
        for i in range(0, len(ratc)):
            if ratc[i] > 0:
                lrac.append(math.log10(ratc[i]))
    else:
        raise ValueError("Either the series is constant or no data was entered.")
    # Do the calculations for fitting a least-squares line. For R/S.
    a = numpy.vstack([ld, numpy.ones(len(ld))]).T
    ba, ma = numpy.linalg.lstsq(a, lra)[0]
    logger.debug("ld: {ld} lra: {lra}".format(ld=ld, lra=lra))
    return ba

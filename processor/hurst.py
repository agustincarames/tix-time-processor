import math

import logging
import numpy
import pywt

NBLK = 5
NLAG = 50
POWER1 = 0.7
POWER2 = 2.5
SHUFFEL = 0
OVERLAP = 1
NDIFF = 0
LAG = 0
CONNECT_ = 0


def crs(data, n, nblk, nlag, overlap, output):
    """
    C version /*Written by Bob Sherman, modified by Walter Willinger, Vadim Teverovsky.*/
    Crs
    S feeds this function a time series (data) of length n.
    The appropriate r and r/s statistics are computed and then
    returned to S through the vector output.

    Python version written by Paula Verghelet

    :param data:
    :param n:
    :param nblk:
    :param nlag:
    :param overlap:
    :param output:
    :return:
    """
    N = n
    NBLK = nblk
    NLAG = nlag
    OVERLAP = overlap
    blksize = 0
    # print "N NBLK NLAG OVERLAP ", N, NBLK, NLAG, OVERLAP
    i = 0
    j = 0
    k = 0
    d = 0
    correction = 0
    NVAL = 0
    increment = 0.0
    temp = 0.0
    min_ = 0.0
    max_ = 0.0
    s = 0.0
    ave = 0.0
    secondmom = 0.0
    xcum = [0 for _ in range(N)]
    xsqcum = [0 for _ in range(N)]
    # Compute xcum's and xsqcum's.
    xcum[0] = data[0]
    xsqcum[0] = data[0] * data[0]
    for i in range(1, N):
        xcum[i] = xcum[i - 1] + data[i]
        xsqcum[i] = xsqcum[i - 1] + data[i] * data[i]
    # Compute r and radj.
    blksize = int(math.floor(N / NBLK))
    if OVERLAP != 0:
        increment = math.log10(float(N)) / NLAG
    else:
        increment = math.log10(float(blksize)) / NLAG
    for k in range(0, NLAG):
        if k == NLAG - 1:
            d = int(math.pow(10.0, float((increment * (k + 1)))))
        else:
            d = int(math.ceil(math.pow(10.0, float((increment * (k + 1))))))
            # print "D ", d
        # d observations used to compute r and radj for lag k.
        correction = int(math.ceil(float(d - blksize) / float(blksize)))
        if correction == NBLK:
            correction -= 1
        if d > blksize:
            NVAL = NBLK - correction
        else:
            NVAL = NBLK
            # print "NVAL k+1 ", NVAL, k+1
        # NVAL is the number of r and radj values computed for lag k.
        # i = 0 is a special case.
        max_ = 0.0
        min_ = 0.0
        ave = (1.0 / d) * xcum[d - 1]
        for j in range(0, d):
            temp = xcum[j] - (j + 1) * ave
            if temp > max_:
                max_ = temp
            elif temp < min_:
                min_ = temp
                # r (k, 0) = max_ - min_
        output[k * NBLK] = max_ - min_
        # print "OUTPUT ", output[k * NBLK]
        secondmom = float(1.0 / d) * xsqcum[d - 1]
        if secondmom > ave * ave:
            s = math.sqrt(secondmom - ave * ave)
            # radj (k, 0) = r (k, 0) / s
            output[NBLK * NLAG + k * NBLK] = float(output[k * NBLK]) / s
            # print output[NBLK * NLAG + k * NBLK]
        else:
            # radj (k, 0) = r (k, 0)
            output[NBLK * NLAG + k * NBLK] = float(output[k * NBLK])
            # print output[NBLK * NLAG + k * NBLK]
        # i > 0
        for i in range(1, NVAL):
            max_ = 0.0
            min_ = 0.0
            ave = (1.0 / d) * (xcum[blksize * i - 1 + d] - xcum[blksize * i - 1])
            # print "AVE ", ave
            for j in range(0, d):
                temp = xcum[blksize * i + j] - xcum[blksize * i - 1] - (j + 1) * ave
                if temp > max_:
                    max_ = temp
                elif temp < min_:
                    min_ = temp
                    # print "TEMP ", temp
            output[k * NBLK + i] = max_ - min_
            secondmom = (1.0 / d) * (xsqcum[blksize * i - 1 + d] - xsqcum[blksize * i - 1])
            # print "SECONDMON", secondmom
            if secondmom > ave * ave:
                s = math.sqrt(secondmom - ave * ave)
                # print "S ", s
                output[NBLK * NLAG + k * NBLK + i] = output[k * NBLK + i] / s
                # print "OUTP ", output[k * NBLK + i]
            else:
                # radj (k, i) = r (k, i)
                output[NBLK * NLAG + k * NBLK + i] = output[k * NBLK + i]
                # print "OUTP ", output[k * NBLK + i]
                # print NBLK * NLAG + k * NBLK + i
                # print output


def rs(data):
    logger = logging.getLogger('plotrs')
    logger.debug("data: {data}".format(data=data))
    n = len(data)
    increment = math.log10(n) / NLAG
    output = [0] * (2 * NBLK * NLAG)
    crs(data, len(data), NBLK, NLAG, OVERLAP, output)
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


def wavelet(data, order=2, octaves_bounds=(2, 8)):
    """
    wavelet <- function(x, length = NULL, order = 2, octave = c(2, 8),
    plotflag = TRUE, title = NULL, description = NULL, output= T)

    A function implemented by Diethelm Wuertz
       dyn.load ("wavedecomp.so")
    Description:
      Function to do the Wavelet estimator of H.

    Arguments:
      x - Data set.
      length - Length of data to be used (must be power of 2)
          if NULL, the previous power will be used
      octave - Beginning and ending octave for estimation.

    Details:
      This method computes the Discrete Wavelet Transform, averages the
      squares of the coefficients of the transform, and then performs a
      linear regression on the logarithm of the average, versus the log
      of j, the scale parameter of the transform. The result should be
      directly proportional to H.
      There are several options available for using this method: method.
      1.  The length of the data must be entered (power of 2).
      2.  c(j1, j2) are the beginning and ending octaves for the estimation.
      3.  'order' is the order of the wavelet. (2 default)
      5.  Calls functions from R's Wavelet package. ( wd, accessD ).
      6.  Inside function, a bound.effect is used in the estimation to
          avoid boundary effects on the coefficients.

    Authors:
      Based on work by Ardry and Flandrin.
      Originally written by Vadim Teverovsky 1997.
    Notes:
      Calls functions from R's package 'wavethresh'

    Notes (Python version):
      Call function for pywt library (http://www.pybytes.com/pywavelets/ref/dwt-discrete-wavelet-transform.html)

    FUNCTION:

    Settings:
    :param data:
    :param order:
    :param octaves_bounds
    :return:
    """
    N = order
    # R:	call = match.call()
    j1 = octaves_bounds[0]
    j2 = octaves_bounds[1]
    # R:	if(is.null(length)) length = 2^floor(log(length(x))/log(2))
    length = int(2 ** math.floor(math.log(len(data), 2)))
    # R:	noctave = log(length, base = 2) - 1
    noctave = int(math.log(length, 2)) - 1
    # R:	bound.effect = ceiling(log(2*N, base = 2))
    bound_effect = int(math.ceil(math.log(2 * N, 2)))
    # R:	statistic = rep(0, noctave)
    statistic = [0] * noctave
    if j2 > noctave - bound_effect:
        # R: cat("Upper bound too high, resetting to ", noctave-bound.effect, "\n")
        # R:	j2 = noctave - bound.effect
        # R:	octave[2] = j2
        j2 = noctave - bound_effect
    # R:	for (j in 1:(noctave - bound.effect)) {
    # R:	    statistic[j] = log(mean((.waccessD(transform,
    # R:	        lev = (noctave+1-j))[N:(2^(noctave+1-j)-N)])^2), base = 2)
    #  db2 = Daubechies filter coefficients, phase 2
    # ppd = periodic
    # wdec = pywt.wavedec(data[0:(int(length))], 'db2', 'ppd', level=int(noctave) + 1)  # esto debería ser noctave - 1?
    wdec = pywt.wavedec(data[:length], 'db2', 'ppd', level=noctave - 1)
    # print "len wdec ", len(wdec)
    # print wdec[8]
    for j in range(0, (noctave - bound_effect)):
        # wdec_level = wdec[int(noctave) + 1 - j][N:(2 ** (int(noctave) + 1 - j) - N)]
        wdec_level = wdec[noctave - 1 - j][N - 1:(2 ** (noctave - j) - N)]
        # print "wdec_level   ", wdec_level
        statistic[j] = math.log(numpy.mean([wdec_level[i] ** 2 for i in range(0, len(wdec_level))]), 2)
    # R: Fit:
    # R:	X = 10^c(j1:j2)
    # R:	Y = 10^statistic[j1:j2]
    # R:	fit = lsfit(log10(X), log10(Y))
    # R:	fitH = lsfit(log10(X), log10(Y*X)/2)
    # R:	diag = as.data.frame(ls.print(fitH, print.it = FALSE)[[2]][[1]])
    # R:	beta = fit$coef[[2]]
    # R:	H = (beta+1)/2

    # Fit:
    X = [10 ** i for i in range(j1, j2 + 1)]
    Y = [10 ** i for i in statistic[j1 - 1:j2]]

    # R:	fit = lsfit(log10(X), log10(Y))
    # R:	fitH = lsfit(log10(X), log10(Y*X)/2)
    x_ = [math.log10(X[i]) for i in range(0, len(X))]
    y_ = [math.log10(Y[i]) for i in range(0, len(Y))]
    yy_ = [math.log10(Y[i] * X[i]) / 2 for i in range(0, len(Y))]

    A = numpy.vstack([x_, numpy.ones(len(X))]).T
    fit, coef1 = numpy.linalg.lstsq(A, y_)[0]

    B = numpy.vstack([x_, numpy.ones(len(X))]).T
    fitH, coef2 = numpy.linalg.lstsq(B, yy_)[0]

    # residuals= numpy.linalg.lstsq(B, yy_)[1]
    # residuals : {(), (1,), (K,)} ndarray
    # Sums of residuals; squared Euclidean 2-norm for each column in b - a*x.
    # If the rank of a is < N or > M, this is an empty array.
    # If b is 1-dimensional, this is a (1,) shape array. Otherwise the shape is (K,).

    beta = fit
    H = (beta + 1) / 2
    return fitH

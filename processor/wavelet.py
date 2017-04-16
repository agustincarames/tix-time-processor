import math
import numpy
import pywt


def wavelet(data):
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
    :param debug:
    :return:
    """
    order = 2
    octave = [2, 8]
    N = order
    # R:	call = match.call()
    j1 = octave[0]
    j2 = octave[1]
    # R:	if(is.null(length)) length = 2^floor(log(length(x))/log(2))
    length = int(2 ** math.floor(math.log(len(data), 2)))
    # R:	noctave = log(length, base = 2) - 1
    noctave = int(math.log(length, 2) - 1)
    # R:	bound.effect = ceiling(log(2*N, base = 2))
    bound_effect = int(math.ceil(math.log(2 * N, 2)))
    # R:	statistic = rep(0, noctave)
    statistic = [0] * noctave
    if j2 > noctave - bound_effect:
        # R: cat("Upper bound too high, resetting to ", noctave-bound.effect, "\n")
        # R:	j2 = noctave - bound.effect
        # R:	octave[2] = j2
        j2 = noctave - bound_effect
        octave[1] = j2
    # R:	for (j in 1:(noctave - bound.effect)) {
    # R:	    statistic[j] = log(mean((.waccessD(transform,
    # R:	        lev = (noctave+1-j))[N:(2^(noctave+1-j)-N)])^2), base = 2)
    #  db2 = Daubechies filter coefficients, phase 2
    # ppd = periodic
    # wdec = pywt.wavedec(data[0:(int(length))], 'db2', 'ppd', level=int(noctave) + 1)  # esto debería ser noctave - 1?
    wdec = pywt.wavedec(data[:length], 'db2', 'ppd')
    # print "len wdec ", len(wdec)
    # print wdec[8]
    for j in range(0, (noctave - bound_effect)):
        # wdec_level = wdec[int(noctave) + 1 - j][N:(2 ** (int(noctave) + 1 - j) - N)]
        wdec_level = wdec[len(wdec) - 1 - j][N:(2 ** (len(wdec) - 1 - j) - N)]
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
    X = [10 ** i for i in range(int(j1), int(j2))]
    Y = [10 ** i for i in statistic[int(j1):int(j2)]]

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

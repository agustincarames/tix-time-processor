import math


def Crs(data, n, nblk, nlag, overlap, output):
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

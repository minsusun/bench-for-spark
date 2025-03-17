package com.omnia.spark.benchmarks;

import com.omnia.spark.benchmarks.tests.LBFGSConf;
import org.apache.spark.graphx.lib.SVDPlusPlus;

public class ConfHelpers {
    public static String SVDPlusPlusConfToString(SVDPlusPlus.Conf conf) {
        return String.format("SVDPlusPlusConf(Rank: %d, MaxIterations: %d, MinVal: %f , MaxVal: %f, Gamma1: %f, Gamma2: %f, Gamma6: %f, Gamma7: %f)", conf.rank(), conf.maxIters(), conf.maxVal(), conf.minVal(), conf.gamma1(), conf.gamma2(), conf.gamma6(), conf.gamma7());
    }

    public static String LBFGSConfToString(LBFGSConf conf) {
        return String.format("LBFGSConf(splitRatio: %f, numCorrections: %d, convergenceTol: %f, maxNumIterations: %d, regParam: %f, seed: %d)", conf.splitRatio(), conf.numCorrections(), conf.convergenceTol(), conf.maxNumIterations(), conf.regParam(), conf.seed());
    }
}

package com.omnia.spark.benchmarks;

import org.apache.spark.graphx.lib.SVDPlusPlus;

public class Helpers {
    public static String SVDPlusPlusConfToString(SVDPlusPlus.Conf conf) {
        return String.format("SVDPlusPlusConf(Rank: %d, MaxIterations: %d, MinVal: %f , MaxVal: %f, Gamma1: %f, Gamma2: %f, Gamma6: %f, Gamma7: %f)", conf.rank(), conf.maxIters(), conf.maxVal(), conf.minVal(), conf.gamma1(), conf.gamma2(), conf.gamma6(), conf.gamma7());
    }
}

/*
 * Crail SQL Benchmarks
 *
 * Author: Animesh Trivedi <atr@zurich.ibm.com>
 *
 * Copyright (C) 2017, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.omnia.spark.benchmarks;

import com.omnia.spark.benchmarks.tests.LBFGSConf;
import org.apache.commons.cli.*;
import org.apache.spark.graphx.lib.SVDPlusPlus;

import java.util.*;

public class ParseOptions {
    private Options options;
    private String banner;
    private String test;
    private String tpcdsQuery;
    private String[] inputFiles;
    private String[] warmupInputFiles;
    private boolean doWarmup;
    private String joinKey;
    private Action action;
    private boolean verbose;
    private String inputFormat;
    private String outputFormat;
    private Map<String, String> inputFormatOptions;
    private Map<String, String> outputFormatOptions;
    private int iterations = 8;
    private String graphLoader = "graphx";
    private boolean auxGraphLoader = false;
    private boolean countGraph = false;
    private boolean naiveImplementation = false;
    private SVDPlusPlus.Conf svdppConf = new SVDPlusPlus.Conf(10, 5, 0.0, 5.0, 0.007, 0.007, 0.005, 0.015);
    private LBFGSConf lbfgsConf = new LBFGSConf(0.6, 10, 1e-4, 20, 0.1, 11L);
    private boolean saveResult = true;

    public ParseOptions() {

        this.banner = " _____  _____ _       ______                 _                          _    \n" +
                "/  ___||  _  | |      | ___ \\               | |                        | |   \n" +
                "\\ `--. | | | | |      | |_/ / ___ _ __   ___| |__  _ __ ___   __ _ _ __| | __\n" +
                " `--. \\| | | | |      | ___ \\/ _ \\ '_ \\ / __| '_ \\| '_ ` _ \\ / _` | '__| |/ /\n" +
                "/\\__/ /\\ \\/' / |____  | |_/ /  __/ | | | (__| | | | | | | | | (_| | |  |   < \n" +
                "\\____/  \\_/\\_\\_____/  \\____/ \\___|_| |_|\\___|_| |_|_| |_| |_|\\__,_|_|  |_|\\_\\\n" +
                "                                                                             \n" +
                "                                                                             ";
        options = new Options();
        options.addOption("h", "help", false, "show help.");
        options.addOption("t", "test", true, "which test to perform, options are (case insensitive): equiJoin, qXXX(tpcds queries), tpcds, readOnly ");
        options.addOption("i", "input", true, "comma separated list of input files/directories. " +
                "EquiJoin takes two files, TPCDS queries takes a tpc-ds data directory, and readOnly take a file or a directory with files");
        options.addOption("w", "warmupInput", true, "warmup files, same semantics as the -i");
        options.addOption("k", "key", true, "key for EquiJoin, default is IntIndex");
        options.addOption("v", "verbose", false, "verbose");
        options.addOption("a", "action", true, "action to take. Your options are (important, no space between ','): \n" +
                " 1. count (default)\n" +
                " 2. collect,items[int, default: 100] \n" +
                " 3. save,filename[str, default: /tmp]");
        options.addOption("if", "inputFormat", true, "input format (where-ever applicable) default: parquet");
        options.addOption("ifo", "inputFormatOptions", true, "input format options as key0,value0,key1,value1...");
        options.addOption("of", "outputFormat", true, "output format (where-ever applicable) default: parquet");
        options.addOption("ofo", "outputFormatOptions", true, "output format options as key0,value0,key1,value1...");
        options.addOption("iter", "iterations", true, "number of iteration for iterative tests, default " + this.iterations);
        options.addOption("gl", "graphLoader", true, "which graph loader to use loading the graph");
        options.addOption("aux", "auxGraphLoader", false, "whether to use auxiliary graph loader");
        options.addOption("count", "", false, "whether to count the vertices and edges of the graph");
        options.addOption("naive", "", false, "use naive implementation if available");
        options.addOption("svdppconf", "", true, "configurations to be used in SVD++(Rank: Int, MaxIterations: Int, MinValue: Double, MaxValue: Double, Gamma1: Double, Gamma2: Double, Gamma6: Double, Gamma7: Double)");
        options.addOption("lbfgsconf", "", true, "configurations to be used in LBFGS(splitRatio: Double, numCorrections: Int, convergenceTol: Double, maxNumIterations: Int, regParam: Double, seed: Long)");
        options.addOption("save", "save", true, "whether to save the results into a file");

        // set defaults
        this.test = "readOnly";
        this.joinKey = "intKey";
        this.inputFormat = "parquet";
        this.outputFormat = "parquet";
        this.verbose = false;
        this.action = new Count();
        this.doWarmup = false;
        this.tpcdsQuery = null;

        this.inputFormatOptions = new HashMap<>(4);
        this.outputFormatOptions = new HashMap<>(4);
        /* at this point we set some defaults */
        // this is for parquet other options are "gzip", "snappy"
        this.outputFormatOptions.putIfAbsent("compression", "none");
    }

    public void show_help() {
        System.out.println(this.banner);
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Main", options);
    }

    private void errorAbort(String str) {
        show_help();
        System.err.println("************ ERROR *******************");
        System.err.println(str);
        System.err.println("**************************************");
        System.exit(-1);
    }

    private void warningKeepGo(String str) {
        System.err.println("************ WARNING *****************");
        System.err.println(str);
        System.err.println("**************************************");
    }

    public void parse(String[] args) {
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("h")) {
                show_help();
                System.exit(0);
            }
            if (cmd.hasOption("t")) {
                this.test = cmd.getOptionValue("t").trim();
                if (this.test.charAt(0) == 'q') {
                    /* specific query test */
                    this.tpcdsQuery = this.test;
                }
                if (this.isTestPageRank() || this.isTestConnectedComponents() ||
                        this.isParquetConversion() || this.isParquetGraphLoadTest() ||
                        this.isLBFGS() || this.isSVDPlusPlus()) {
                    /* for page rank, we have no op */
                    this.action = new Noop();
                }
            }
            if (cmd.hasOption("iter")) {
                this.iterations = Integer.parseInt(cmd.getOptionValue("iter").trim());
            }
            if (cmd.hasOption("v")) {
                this.verbose = true;
            }
            if (cmd.hasOption("k")) {
                this.joinKey = cmd.getOptionValue("k").trim();
            }
            if (cmd.hasOption("if")) {
                this.inputFormat = cmd.getOptionValue("if").trim();
                if (this.inputFormat.compareToIgnoreCase("nullio") == 0) {
                    this.inputFormat = "org.apache.spark.sql.NullFileFormat";
                }
            }
            if (cmd.hasOption("of")) {
                this.outputFormat = cmd.getOptionValue("of").trim();
                if (this.outputFormat.compareToIgnoreCase("nullio") == 0) {
                    this.inputFormat = "org.apache.spark.sql.NullFileFormat";
                }
            }
            if (cmd.hasOption("ofo")) {
                String[] one = cmd.getOptionValue("ofo").trim().split(",");
                if (one.length % 2 != 0) {
                    errorAbort("Illegal format for outputFormatOptions. Number of parameters " + one.length + " are not even");
                }
                for (int i = 0; i < one.length; i += 2) {
                    this.outputFormatOptions.put(one[i].trim(), one[i + 1].trim());
                }
            }
            if (cmd.hasOption("ifo")) {
                String[] one = cmd.getOptionValue("ifo").trim().split(",");
                if (one.length % 2 != 0) {
                    errorAbort("Illegal format for inputFormatOptions. Number of parameters " + one.length + " are not even");
                }
                for (int i = 0; i < one.length; i += 2) {
                    this.inputFormatOptions.put(one[i].trim(), one[i + 1].trim());
                }
            }

            if (cmd.hasOption("i")) {
                // get the value and split it
                this.inputFiles = Arrays.stream(cmd.getOptionValue("i").split(","))
                        .map(String::trim).toArray(String[]::new);
            }
            if (cmd.hasOption("w")) {
                // get the value and split it
                this.warmupInputFiles = Arrays.stream(cmd.getOptionValue("w").split(","))
                        .map(String::trim).toArray(String[]::new);
                this.doWarmup = true;
            }
            if (cmd.hasOption("a")) {
                String[] tokens = cmd.getOptionValue("a").split(",");
                if (tokens.length == 0) {
                    errorAbort("Failed to parse command line properties " + cmd.getOptionValue("a"));
                }
                if (tokens[0].compareToIgnoreCase("count") == 0) {
                    this.action = new Count();
                } else if (tokens[0].compareToIgnoreCase("collect") == 0) {
                    int items = 0;
                    if (tokens.length != 2) {
                        items = 100;
                    } else {
                        items = Integer.parseInt(tokens[1].trim());
                    }
                    this.action = new Collect(items);
                } else if (tokens[0].compareToIgnoreCase("save") == 0) {
                    String fileName = (tokens.length >= 2) ? tokens[1].trim() : "/sql-benchmark-output";
                    this.action = new Save(fileName);
                } else {
                    errorAbort("ERROR: illegal action name : " + tokens[0]);
                }
            }
            if (cmd.hasOption("aux")) {
                this.auxGraphLoader = true;
            }
            if (cmd.hasOption("gl")) {
                this.graphLoader = cmd.getOptionValue("gl").trim().toLowerCase();
            } else {
                if (this.inputFiles[0].endsWith(".parquet")) {
                    this.graphLoader = "parquet";
                }
            }
            if (cmd.hasOption("count")) {
                this.countGraph = true;
            }
            if (cmd.hasOption("naive")) {
                this.naiveImplementation = true;
            }
            if (cmd.hasOption("svdppconf")) {
                String[] arr = Arrays.stream(cmd.getOptionValue("svdppconf").trim().split(","))
                        .map(String::trim).toArray(String[]::new);
                try {
                    if (arr.length != 8) {
                        throw new ParseException("Illegal number of parameters " + arr.length + ". It should be 8(Rank: Int, MaxIterations: Int, MinValue: Double, MaxValue: Double, Gamma1: Double, Gamma2: Double, Gamma6: Double, Gamma7: Double)");
                    }
                    int rank = Integer.parseInt(arr[0]);
                    int maxIterations = Integer.parseInt(arr[1]);
                    double minVal = Double.parseDouble(arr[2]);
                    double maxVal = Double.parseDouble(arr[3]);
                    double gamma1 = Double.parseDouble(arr[4]);
                    double gamma2 = Double.parseDouble(arr[5]);
                    double gamma6 = Double.parseDouble(arr[6]);
                    double gamma7 = Double.parseDouble(arr[7]);
                    svdppConf = new SVDPlusPlus.Conf(rank, maxIterations, minVal, maxVal, gamma1, gamma2, gamma6, gamma7);
                } catch (Exception e) {
                    warningKeepGo("WARNING: " + e.getMessage() + "\n" + "Fall back to default values: " + ConfHelpers.SVDPlusPlusConfToString(svdppConf));
                }
            }
            if (cmd.hasOption("lbfgsconf")) {
                String[] arr = Arrays.stream(cmd.getOptionValue("lbfgsconf").trim().split(","))
                        .map(String::trim).toArray(String[]::new);
                try {
                    if (arr.length != 6) {
                        throw new ParseException("Illegal number of parameters " + arr.length + ". It should be 8(splitRatio: Double, numCorrections: Int, convergenceTol: Double, maxNumIterations: Int, regParam: Double, seed: Long)");
                    }
                    double splitRatio = Double.parseDouble(arr[0]);
                    int numCorrections = Integer.parseInt(arr[1]);
                    double convergenceTol = Double.parseDouble(arr[2]);
                    int maxNumIterations = Integer.parseInt(arr[3]);
                    double regParam = Double.parseDouble(arr[4]);
                    long seed = Long.parseLong(arr[5]);
                    lbfgsConf = new LBFGSConf(splitRatio, numCorrections, convergenceTol, maxNumIterations, regParam, seed);
                } catch (Exception e) {
                    warningKeepGo("WARNING: " + e.getMessage() + "\n" + "Fall back to default values: " + ConfHelpers.LBFGSConfToString(lbfgsConf));
                }
            }
            if (cmd.hasOption("save")) {
                this.saveResult = Boolean.parseBoolean(cmd.getOptionValue("save"));
            }

        } catch (ParseException e) {
            errorAbort("Failed to parse command line properties" + e);
        }
        // if not files are set
        if (this.inputFiles == null) {
            errorAbort("ERROR:" + " please specify some input files for the SQL test");
        }
        // check valid test names
        if (!isTestEquiJoin() && !isTestQuery() && !isTestTPCDS() &&
                !isTestReadOnly() && !isTestPageRank() && !isTestConnectedComponents() &&
                !isParquetConversion() && !isParquetGraphLoadTest() && !isSVDPlusPlus() &&
                !isLBFGS()) {
            errorAbort("ERROR: illegal test name : " + this.test);
        }
        /* some sanity checks */
        if (isTestEquiJoin() && this.inputFiles.length != 2) {
            errorAbort("ERROR:" + this.test + " needs two files as inputs");
        }
    }

    public boolean isTestEquiJoin() {
        return this.test.compareToIgnoreCase("EquiJoin") == 0;
    }

    public boolean isTestQuery() {
        return !(this.tpcdsQuery == null);
    }

    public boolean isTestTPCDS() {
        return (this.test.compareToIgnoreCase("tpcds") == 0);
    }

    public boolean isTestReadOnly() {
        return this.test.compareToIgnoreCase("readOnly") == 0;
    }

    public boolean isTestPageRank() {
        return this.test.compareToIgnoreCase("pagerank") == 0;
    }

    public boolean isTestConnectedComponents() {
        return this.test.compareToIgnoreCase("cc") == 0;
    }

    public boolean isParquetConversion() {
        return this.test.compareToIgnoreCase("parquet") == 0 || this.test.compareToIgnoreCase("parquetconversion") == 0;
    }

    public boolean isParquetGraphLoadTest() {
        return this.test.compareToIgnoreCase("parquetGraphLoadTest") == 0;
    }

    public boolean isSVDPlusPlus() {
        return this.test.compareToIgnoreCase("svdPlusPlus") == 0 || this.test.compareToIgnoreCase("svdpp") == 0;
    }

    public boolean isLBFGS() {
        return this.test.compareToIgnoreCase("LBFGS") == 0;
    }

    public String[] getInputFiles() {
        return this.inputFiles;
    }

    public void setInputFiles(String[] input) {
        this.inputFiles = input;
    }

    public String[] getWarmupInputFiles() {
        return this.warmupInputFiles;
    }

    public boolean getDoWarmup() {
        return this.doWarmup;
    }

    public String getJoinKey() {
        return this.joinKey;
    }

    public Action getAction() {
        return this.action;
    }

    public boolean getVerbose() {
        return this.verbose;
    }

    public String getInputFormat() {
        return this.inputFormat;
    }

    public String getOutputFormat() {
        return this.outputFormat;
    }

    public Map<String, String> getInputFormatOptions() {
        return this.inputFormatOptions;
    }

    public Map<String, String> getOutputFormatOptions() {
        return this.outputFormatOptions;
    }

    public String getTPCDSQuery() {
        return this.tpcdsQuery;
    }

    public int getIterations() {
        return this.iterations;
    }

    public boolean getAuxGraphLoader() {
        return this.auxGraphLoader;
    }

    public String getGraphLoader() {
        return this.graphLoader;
    }

    public boolean getCountGraph() {
        return this.countGraph;
    }

    public boolean getNaiveImplementation() {
        return this.naiveImplementation;
    }

    public SVDPlusPlus.Conf getSVDPlusPlusConf() {
        return this.svdppConf;
    }

    public LBFGSConf getLBFGSConf() {
        return this.lbfgsConf;
    }

    public boolean getSaveResult() {
        return this.saveResult;
    }
}

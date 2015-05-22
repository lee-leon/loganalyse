/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Leon Lee @ lee.leon0519@gmail.com
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *  
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

/**
 * File Name   : LogStatistic.java
 * Author      : Leon Lee
 * Mail        : lee.leon0519@gmail.com
 * Created Time: May 13, 2015 1:58:00 PM
 * Description :  Package is used to split nginx log file in HDFS and
 * 				  store the splices into HBase Table.
 */
package com.leon.hadoop.loganalyse;

import org.apache.spark.api.java.JavaRDD;
import java.util.regex.Pattern;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;


public class LogStatistic {
	public static void main(String[] args) {
		
		String logFile = "hdfs://homeyard.com:9000/log/nginx/access/go1978.com/20150429.1430299110166.log";
		SparkConf conf = new SparkConf().setAppName("Nginx Log Statics");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = sc.textFile(logFile).cache();
		
		long numAs = logData.filter(new Function<String, Boolean>() {
				public Boolean call(String s) {return s.contains("a");}
	}).count();
		long numBs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {return s.contains("b");}
		}).count();
		System.out.println("Lines with a: " + numAs + ", lines with b: " +numBs);
	}
}

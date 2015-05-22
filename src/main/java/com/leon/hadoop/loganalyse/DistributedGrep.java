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
 * File Name   : DistributedGrep.java
 * Author      : Leon Lee
 * Mail        : lee.leon0519@gmail.com
 * Created Time: May 18, 2015 10:06:57 AM
 * Description : 
 */
package com.leon.hadoop.loganalyse;


import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DistributedGrep extends Configured implements Tool {
  public static final String REGEX_KEY = "com.eftimoff.mapreduce.filtering.filtering.regex";
  
  public static class GrepMapper extends Mapper<Object, Text, NullWritable, Text> {
    private Pattern pattern = null;

    public void setup(Context context) throws IOException, InterruptedException {
      pattern = Pattern.compile(context.getConfiguration().get(REGEX_KEY, ""));
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      Matcher matcher = pattern.matcher(value.toString());
      if (matcher.find()) {
        context.write(NullWritable.get(), value);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new DistributedGrep(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    String[] otherArgs = parser.getRemainingArgs();
    if (otherArgs.length != 3) {
      System.err.println("Usage: DistributedGrep <regex> <in> <out>");
      ToolRunner.printGenericCommandUsage(System.err);
      System.exit(2);
    }
    @SuppressWarnings("deprecation")
	Job job = new Job(conf, "Distributed Grep");
    job.setJarByClass(DistributedGrep.class);
    job.setMapperClass(GrepMapper.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.getConfiguration().set(REGEX_KEY, otherArgs[0]);
    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
    boolean success = job.waitForCompletion(true);

    return success ? 0 : 1;
  }
}
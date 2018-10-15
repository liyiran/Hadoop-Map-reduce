/*
 * $Id$
 *
 * Copyright (c) 2003, 2004 WorldTicket A/S
 * All rights reserved.
 */
package liyiran;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Yiran Li / 2M business applications a|s
 * @version $Revision$ $Date$
 */
public class InvertIndex extends Configured implements Tool {
    public static void main(String... args) throws Exception {
        if (args.length != 2) {
            System.err.println("argument wrong");
            System.exit(-1);
        }
        int res = ToolRunner.run(new InvertIndex(), args);
        System.exit(res);
    }


    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(InvertIndex.class);
        job.setJobName("word count");
        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
        job.setMapperClass(InvertIndexMapper.class);
        job.setReducerClass(InvertIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
        return (job.waitForCompletion(true) ? 0 : 1);
    }
}

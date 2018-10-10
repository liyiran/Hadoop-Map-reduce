/*
 * $Id$
 *
 * Copyright (c) 2003, 2004 WorldTicket A/S
 * All rights reserved.
 */
package liyiran;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author Yiran Li / 2M business applications a|s
 * @version $Revision$ $Date$
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    protected Text documentName = new Text();
    protected Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().split("\\t");
        documentName.set(lines[0]);
        StringTokenizer tokenizer = new StringTokenizer(lines[1]);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, documentName);
        }
    }
}

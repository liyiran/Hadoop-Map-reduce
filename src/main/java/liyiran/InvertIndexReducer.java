/*
 * $Id$
 *
 * Copyright (c) 2003, 2004 WorldTicket A/S
 * All rights reserved.
 */
package liyiran;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Yiran Li / 2M business applications a|s
 * @version $Revision$ $Date$
 */
public class InvertIndexReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Multiset<String> counter = HashMultiset.create();
        for (Text value : values) {
            counter.add(value.toString());
        }
        StringBuilder buffer = new StringBuilder();
        for(String document: counter.elementSet()) {
            if(buffer.length() != 0) {
                buffer.append(" ");
            }
            buffer.append(document).append(":").append(counter.count(document));
        }
        context.write(key, new Text(buffer.toString()));
    }

}

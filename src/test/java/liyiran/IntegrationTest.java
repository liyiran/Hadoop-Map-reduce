/*
 * $Id$
 *
 * Copyright (c) 2003, 2004 WorldTicket A/S
 * All rights reserved.
 */
package liyiran;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @author Yiran Li / 2M business applications a|s
 * @version $Revision$ $Date$
 */
public class IntegrationTest {
    private static Configuration conf;
    private static Path input;
    private static Path output;
    private static FileSystem fs;

    @BeforeAll
    public static void setup() throws IOException {
        conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");

        input = new Path("src/test/resources/testdata");
        output = new Path("target/output");

        fs = FileSystem.getLocal(conf);
        fs.delete(output, true);
    }

    @Test
    public void test() throws Exception {
        InvertIndex invertIndex = new InvertIndex();
        invertIndex.setConf(conf);

        int exitCode = invertIndex.run(new String[]{input.toString(), output.toString()});
        assertEquals(0, exitCode);

        validateOuput();
    }

    private void validateOuput() throws IOException {
        InputStream in = null;
        try {
            in = fs.open(new Path("target/output/part-r-00000"));

            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            assertEquals("hello\t1:1 2:1", br.readLine());
            assertEquals("liyiran\t2:1 3:1", br.readLine());
            assertEquals("world\t1:1 3:1", br.readLine());

        } finally {
            IOUtils.closeStream(in);
        }
    }
}

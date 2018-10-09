package liyiran;/*
 * $Id$
 *
 * Copyright (c) 2003, 2004 WorldTicket A/S
 * All rights reserved.
 */

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

/**
 * @author Yiran Li / 2M business applications a|s
 * @version $Revision$ $Date$
 */
//@ExtendWith(MockitoExtension.class)
public class JobTest {
    @Mock
    private Mapper.Context mockContext;
    private WordCountMapper mapper;
    private IntWritable one;
    @BeforeEach
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        mapper = new WordCountMapper();
        mapper.word = mock(Text.class);
        one = new IntWritable(1);
    }

    @Test
    public void testMapper() throws InterruptedException, IOException, ClassNotFoundException {
        String line = "hello world a world";
        mapper.map(null, new Text(line), mockContext);
        InOrder inOrder = inOrder(
                mapper.word, mockContext,
                mapper.word, mockContext, 
                mapper.word, mockContext, 
                mapper.word, mockContext);

        assertCountedOnce(inOrder, "hello");
        assertCountedOnce(inOrder, "world");
        assertCountedOnce(inOrder, "a");
        assertCountedOnce(inOrder, "world");
    }

    @AfterEach
    public void tearDown() throws Exception {
  
    }

    private void assertCountedOnce(InOrder inOrder, String w) throws IOException, InterruptedException {
        inOrder.verify(mapper.word).set(eq(w));
        inOrder.verify(mockContext).write(eq(mapper.word), eq(one));
    }
}


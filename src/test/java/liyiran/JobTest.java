package liyiran;/*
 * $Id$
 *
 * Copyright (c) 2003, 2004 WorldTicket A/S
 * All rights reserved.
 */

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author Yiran Li / 2M business applications a|s
 * @version $Revision$ $Date$
 */
//@ExtendWith(MockitoExtension.class)
public class JobTest {
    @Mock
    private Mapper.Context mapperContext;
    @Mock
    private Reducer.Context reducerContext;
    private WordCountMapper mapper;
    private Text documentNumber;
    private WordCountReducer reducer;

    @BeforeEach
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        mapper = new WordCountMapper();
        mapper.word = mock(Text.class);
        documentNumber = new Text("111");
        reducer = new WordCountReducer();
        System.out.println("hahaha");
    }

    @Test
    public void testMapper() throws InterruptedException, IOException, ClassNotFoundException {
        String line = "111\thello world a world";
        mapper.map(null, new Text(line), mapperContext);
        InOrder inOrder = inOrder(
                mapper.word, mapperContext,
                mapper.word, mapperContext,
                mapper.word, mapperContext,
                mapper.word, mapperContext);

        assertCountedOnce(inOrder, "hello");
        assertCountedOnce(inOrder, "world");
        assertCountedOnce(inOrder, "a");
        assertCountedOnce(inOrder, "world");
    }

    @Test
    public void testReducer() throws IOException, InterruptedException {
        List<Text> values = Arrays.asList(new Text("111"), new Text("222"), new Text("333"));
        reducer.reduce(new Text("foo"), values, reducerContext);
        verify(reducerContext).write(new Text("foo"), new Text("111:1 222:1 333:1"));
    }

    @AfterEach
    public void tearDown() throws Exception {

    }

    private void assertCountedOnce(InOrder inOrder, String w) throws IOException, InterruptedException {
        inOrder.verify(mapper.word).set(eq(w));
        inOrder.verify(mapperContext).write(eq(mapper.word), eq(documentNumber));
    }
}


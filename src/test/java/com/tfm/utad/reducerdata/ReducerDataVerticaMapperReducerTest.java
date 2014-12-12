package com.tfm.utad.reducerdata;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class ReducerDataVerticaMapperReducerTest {

    private MapDriver<Object, Text, Text, ReducerVerticaValue> mapDriver;
    private ReduceDriver<Text, ReducerVerticaValue, Text, NullWritable> reduceDriver;
    private MapReduceDriver<Object, Text, Text, ReducerVerticaValue, Text, NullWritable> mapReduceDriver;
    private SimpleDateFormat sdf;

    @Before
    public void setUp() {
        ReducerDataVerticaMapper mapper = new ReducerDataVerticaMapper();
        ReducerDataVerticaReducer reducer = new ReducerDataVerticaReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        sdf = new SimpleDateFormat(ReducerConstants.FORMATTER_DATE_YEAR_MONTH_DATE_HOURS_MINUTES_SECONDS);
    }

    @Test
    public void testMapper() throws IOException, ParseException {
        String key = "123456";
        Date date = sdf.parse("2014-12-06 17:43:21");
        mapDriver.withInput(new Text(key), new Text(
                "40.48989;-3.65754;User189;2014-12-06 17:43:21;20141206-34567-189"));
        mapDriver.withOutput(new Text("User189" + "20141206-34567-189"), new ReducerVerticaValue(new LongWritable((long) 123456), new Text("User189"), date, new Text("20141206-34567-189"), new DoubleWritable(Double.valueOf("40.48989")), new DoubleWritable(Double.valueOf("-3.65754")), new LongWritable(new Long("189"))));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException, ParseException {
        List<ReducerVerticaValue> values = new ArrayList<>();
        Date date = sdf.parse("2014-12-06 17:43:21");
        ReducerVerticaValue verticaValue = new ReducerVerticaValue(new LongWritable((long) 123456), new Text("User189"), date, new Text("20141206-34567-189"), new DoubleWritable(Double.valueOf("40.48989")), new DoubleWritable(Double.valueOf("-3.65754")), new LongWritable(new Long("189")));
        values.add(verticaValue);
        reduceDriver.withInput(new Text("User189" + "20141206-34567-189"), values);
        reduceDriver.withOutput(new Text(values.get(0).toString()), NullWritable.get());
        reduceDriver.runTest();
    }
}
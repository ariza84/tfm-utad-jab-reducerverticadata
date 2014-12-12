/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.tfm.utad.reducerdata;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReducerDataVerticaMapper extends
        Mapper<Object, Text, Text, ReducerVerticaValue> {
 
    private static SimpleDateFormat sdf;
    
    private final static Logger LOG = LoggerFactory.getLogger(ReducerDataVerticaMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        sdf = new SimpleDateFormat(ReducerConstants.FORMATTER_DATE_YEAR_MONTH_DATE_HOURS_MINUTES_SECONDS);
    }
 
    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] values = value.toString().replaceAll("\n", "").split(ReducerConstants.SPLIT_SEMICOLON);
        Double latitude = Double.valueOf(values[0]);
        Double longitude = Double.valueOf(values[1]);
        String user = values[2];
        Date date;
        try {
            date = sdf.parse(values[3]);
        } catch (ParseException ex) {
            LOG.error("Error parsing date: " + ex.toString());
            date = null;
        }
        String activity = values[4];
        if (isValidKey(latitude, longitude, user)) {
            Text text = (Text)key;
            Long id = Long.valueOf(text.toString());
            Long userid = Long.valueOf(user.substring(4));
            context.write(new Text(user + activity), 
                    new ReducerVerticaValue(new LongWritable(id), new Text(user), date, new Text(activity), new DoubleWritable(latitude), new DoubleWritable(longitude), new LongWritable(userid)));
        } else {
            LOG.error("Invalid values in line: " + value.toString());
            context.getCounter(ReducerDataEnum.MALFORMED_DATA).increment(1);
        }
    }
    
    private boolean isValidKey(Double latitude, Double longitude, String user) {
        return latitude != null && longitude != null && user != null;
    }
}
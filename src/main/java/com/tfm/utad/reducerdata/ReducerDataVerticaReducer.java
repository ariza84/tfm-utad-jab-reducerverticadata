/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tfm.utad.reducerdata;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerDataVerticaReducer extends
        Reducer<Text, ReducerVerticaValue, Text, NullWritable> {

    @Override
    public void reduce(Text key, Iterable<ReducerVerticaValue> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        for (ReducerVerticaValue value : values) {
            if (count == 0) {
                context.write(new Text(value.toString()), NullWritable.get());
            } else {
                if (count % 10 == 0) {
                    context.write(new Text(value.toString()), NullWritable.get());
                }
            }
            count++;
        }
    }
}

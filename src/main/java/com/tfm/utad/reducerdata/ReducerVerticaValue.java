/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tfm.utad.reducerdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReducerVerticaValue implements WritableComparable, Serializable {

    private LongWritable offset = new LongWritable();
    private Text user = new Text();
    private Date date;
    private Text activity = new Text();
    private DoubleWritable latitude = new DoubleWritable();
    private DoubleWritable longitude = new DoubleWritable();
    private LongWritable userid = new LongWritable();

    private final SimpleDateFormat sdf = new SimpleDateFormat(ReducerConstants.FORMATTER_DATE_YEAR_MONTH_DATE_HOURS_MINUTES_SECONDS);
    private final static Logger LOG = LoggerFactory.getLogger(ReducerVerticaValue.class);

    public ReducerVerticaValue() {

    }

    public ReducerVerticaValue(LongWritable offset, Text user, Date date, Text activity, DoubleWritable latitude, DoubleWritable longitude, LongWritable userid) {
        this.offset = offset;
        this.user = user;
        this.date = date;
        this.activity = activity;
        this.latitude = latitude;
        this.longitude = longitude;
        this.userid = userid;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        offset.write(out);
        user.write(out);
        out.writeUTF(sdf.format(date));
        activity.write(out);
        latitude.write(out);
        longitude.write(out);
        userid.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        offset.readFields(in);
        user.readFields(in);
        try {
            date = sdf.parse(in.readUTF());
        } catch (ParseException ex) {
            LOG.error("Error parsing date: " + ex.toString());
            date = null;
        }
        activity.readFields(in);
        latitude.readFields(in);
        longitude.readFields(in);
        userid.readFields(in);
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 19 * hash + Objects.hashCode(this.offset);
        hash = 19 * hash + Objects.hashCode(this.user);
        hash = 19 * hash + Objects.hashCode(this.date);
        hash = 19 * hash + Objects.hashCode(this.activity);
        hash = 19 * hash + Objects.hashCode(this.latitude);
        hash = 19 * hash + Objects.hashCode(this.longitude);
        hash = 19 * hash + Objects.hashCode(this.userid);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ReducerVerticaValue other = (ReducerVerticaValue) obj;
        if (!Objects.equals(this.offset, other.offset)) {
            return false;
        }
        if (!Objects.equals(this.user, other.user)) {
            return false;
        }
        if (!Objects.equals(this.date, other.date)) {
            return false;
        }
        if (!Objects.equals(this.activity, other.activity)) {
            return false;
        }
        if (!Objects.equals(this.latitude, other.latitude)) {
            return false;
        }
        if (!Objects.equals(this.longitude, other.longitude)) {
            return false;
        }
        return Objects.equals(this.userid, other.userid);
    }

    @Override
    public String toString() {
        return offset + ReducerConstants.SEPARATOR_TAB + user + ReducerConstants.SEPARATOR_TAB + sdf.format(date) + ReducerConstants.SEPARATOR_TAB + activity + ReducerConstants.SEPARATOR_TAB + latitude + ReducerConstants.SEPARATOR_TAB + longitude + ReducerConstants.SEPARATOR_TAB + userid;
    }

    @Override
    public int compareTo(Object o) {
        ReducerVerticaValue other = (ReducerVerticaValue) o;
        return this.offset.compareTo(other.offset);
    }
}
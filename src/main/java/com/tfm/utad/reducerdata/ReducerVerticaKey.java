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
import java.util.Objects;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ReducerVerticaKey implements WritableComparable, Serializable {

    private Text user = new Text();
    private Text activity = new Text();

    public ReducerVerticaKey() {

    }

    public ReducerVerticaKey(Text user, Text activity) {
        this.user = user;
        this.activity = activity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        user.write(out);
        activity.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        user.readFields(in);
        activity.readFields(in);
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 19 * hash + Objects.hashCode(this.user);
        hash = 19 * hash + Objects.hashCode(this.activity);
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
        final ReducerVerticaKey other = (ReducerVerticaKey) obj;
        if (!Objects.equals(this.user, other.user)) {
            return false;
        }
        return Objects.equals(this.activity, other.activity);
    }

    @Override
    public String toString() {
        return user + ReducerConstants.SEPARATOR_TAB + activity;
    }

    @Override
    public int compareTo(Object o) {
        ReducerVerticaKey other = (ReducerVerticaKey) o;
        int usr = this.user.compareTo(other.user);
        if ( usr == 0) {
            return this.activity.compareTo(other.activity);
        }
        return usr;
    }
}
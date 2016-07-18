package com.trooper.storm.hdfs;

import backtype.storm.tuple.Tuple;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.joda.time.DateTime;

/**
 * hdfsolt FileRotationPolicy example
 * rotate hdfs files evevyday
 */
public class DailyRotationPolicy implements FileRotationPolicy {
    private String currentDate;
    private String dateFormat = "yyyy-MM-dd";

    public DailyRotationPolicy() {
        DateTime dt = new DateTime();
        currentDate = dt.toString(dateFormat);
    }
    @Override
    public boolean mark(Tuple tuple, long offset) {
        DateTime dt = new DateTime();
        String nowdate = dt.toString(dateFormat);
        if (StringUtils.equals(currentDate, nowdate)) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void reset() {
        DateTime dt = new DateTime();
        currentDate = dt.toString(dateFormat);
    }
}

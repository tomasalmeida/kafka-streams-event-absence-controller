package com.demo.streams.sessionwindow;

import com.demo.streams.DeviceAlert;
import org.apache.kafka.streams.kstream.Aggregator;
import org.checkerframework.checker.units.qual.A;

public class WindowAggregator implements Aggregator<String, Long, String> {

    @Override
    public String apply(String key, Long value, String aggregate) {
        return "";
    }
}

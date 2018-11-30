package com.jouryu.storm.spout.scheme;

import com.jouryu.constants.StormContants;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;

/**
 * Created by tomorrow on 18/11/21.
 */

public class KafkaMessageScheme implements Scheme {

    private static final String[] fields = {
        StormContants.KAFKA_TIME_FIELD,
        StormContants.KAFKA_SENSOR_TYPE_FIELD,
        StormContants.KAFKA_VALUE_FIELD
    };

    @Override
    public List<Object> deserialize(ByteBuffer ser) {
        String[] msg;
        List<Object> ret = null;
        CharsetDecoder decoder= Charset.forName("UTF-8").newDecoder();
        try {
            msg = decoder.decode(ser.asReadOnlyBuffer()).toString().split(":");
            ret = new Values(msg[0], msg[1], msg[2]);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(fields);
    }
}
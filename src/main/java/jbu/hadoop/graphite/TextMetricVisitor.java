package jbu.hadoop.graphite;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsVisitor;

public class TextMetricVisitor implements MetricsVisitor {

    private String value;

    public String getValue() {
        return value;
    }

    @Override
    public void gauge(MetricsInfo info, int value) {
        this.value = Integer.toString(value);
    }

    @Override
    public void gauge(MetricsInfo info, long value) {
        this.value = Long.toString(value);
    }

    @Override
    public void gauge(MetricsInfo info, float value) {
        this.value = Float.toString(value);
    }

    @Override
    public void gauge(MetricsInfo info, double value) {
        this.value = Double.toString(value);
    }

    @Override
    public void counter(MetricsInfo info, int value) {
        this.value = Integer.toString(value);
    }

    @Override
    public void counter(MetricsInfo info, long value) {
        this.value = Long.toString(value);
    }

}

package jbu.hadoop.graphite;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsVisitor;
import org.python.core.PyFloat;
import org.python.core.PyInteger;
import org.python.core.PyLong;
import org.python.core.PyObject;


public class PickleMetricVisitor implements MetricsVisitor {

    private PyObject pyvalue;

    public PyObject getPyvalue() {
        return pyvalue;
    }

    @Override
    public void gauge(MetricsInfo info, int value) {
        this.pyvalue = new PyInteger(value);
    }

    @Override
    public void gauge(MetricsInfo info, long value) {
        this.pyvalue = new PyLong(value);
    }

    @Override
    public void gauge(MetricsInfo info, float value) {
        this.pyvalue = new PyFloat(value);
    }

    @Override
    public void gauge(MetricsInfo info, double value) {
        this.pyvalue = new PyFloat(value);
    }

    @Override
    public void counter(MetricsInfo info, int value) {
        this.pyvalue = new PyInteger(value);
    }

    @Override
    public void counter(MetricsInfo info, long value) {
        this.pyvalue = new PyLong(value);
    }

}

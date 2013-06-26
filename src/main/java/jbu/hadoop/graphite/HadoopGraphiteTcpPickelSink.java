package jbu.hadoop.graphite;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.net.DNS;
import org.python.core.*;
import org.python.modules.cPickle;

import java.net.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class HadoopGraphiteTcpPickelSink implements org.apache.hadoop.metrics2.MetricsSink {

    private static final Log LOG = LogFactory.getLog(HadoopGraphiteTcpPickelSink.class);

    public static final String SERVERS_PROPERTY = "server";
    public static final String SERVERS_PORT_PROPERTY = "port";
    public static final String PREFIX_PROPERTY = "prefix";
    public static final int DEFAULT_PORT = 2004;
    public static final String DEFAULT_HOSTNAME = "#hostname#";
    public static final String DEFAULT_PREFIX = "hadoop";

    private InetSocketAddress inetSocketAddress;

    private String hostName;
    private String carbonHost = "localhost";
    private int carbonPort = DEFAULT_PORT;
    private String prefix = DEFAULT_PREFIX;

    private PickleMetricVisitor pickleMetricVisitor = new PickleMetricVisitor();

    @Override
    public void putMetrics(MetricsRecord metricsRecord) {
        LOG.debug("Export to " + carbonHost + ":" + carbonPort + " results" + metricsRecord);
        try {
            byte[] buf = createPicklePacket(metricsRecord);

            // Send Pickle Object by TCP

        } catch (Exception e) {
            LOG.warn("Failure to send result to graphite server '" + carbonHost + ":" + carbonPort, e);
        }
    }

    private byte[] createPicklePacket(MetricsRecord metricsRecord) {
        PyList list = new PyList();

        String metricName = metricsRecord.name();
        String contextName = metricsRecord.context();

        // Convert to second
        int time = (int) metricsRecord.timestamp() / 1000;

        for (AbstractMetric am : metricsRecord.metrics()) {
            StringBuffer groupName = new StringBuffer();
            am.visit(pickleMetricVisitor);
            groupName.append(prefix);
            groupName.append(".");
            groupName.append(hostName);
            groupName.append(".");
            groupName.append(contextName);
            groupName.append(".");
            groupName.append(metricName);
            groupName.append(".");
            groupName.append(am.name());

            PyObject pyValue = pickleMetricVisitor.getPyvalue();
            list.add(new PyTuple(new PyString(groupName.toString()), new PyTuple(new PyInteger(time), pyValue)));
        }

        PyString payload = cPickle.dumps(list);
        byte[] bp = payload.toBytes();
        ByteBuffer buf = ByteBuffer.allocate(4 + bp.length);
        buf.putInt(payload.__len__()).array();
        buf.put(bp);
        return buf.array();
    }

    @Override
    public void flush() {
        // nothing to do as we are not buffering data
    }

    @Override
    public void init(SubsetConfiguration conf) {
        LOG.debug("Initializing the Graphite metrics.");
        // Configure carbon connection
        // get the node name

        // Take the hostname from the DNS class.
        if (conf.getString("slave.host.name") != null) {
            hostName = conf.getString("slave.host.name");
        } else {
            try {
                hostName = DNS.getDefaultHost(
                        conf.getString("dfs.datanode.dns.interface", "default"),
                        conf.getString("dfs.datanode.dns.nameserver", "default"));
            } catch (UnknownHostException uhe) {
                LOG.error(uhe);
                hostName = DEFAULT_HOSTNAME;
            }
        }

        if (conf.containsKey(PREFIX_PROPERTY)) {
            prefix = conf.getString(PREFIX_PROPERTY);
        }

        if (conf.containsKey(SERVERS_PROPERTY)) {
            carbonHost = conf.getString(SERVERS_PROPERTY);
        }
        if (conf.containsKey(SERVERS_PORT_PROPERTY)) {
            carbonPort = conf.getInt(SERVERS_PORT_PROPERTY, DEFAULT_PORT);
        }

        LOG.info("Init graphite receiver : " + carbonHost + ":" + carbonPort);
        inetSocketAddress = new InetSocketAddress(carbonHost, carbonPort);
    }
}

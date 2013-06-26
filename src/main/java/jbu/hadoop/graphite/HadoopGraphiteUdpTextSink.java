package jbu.hadoop.graphite;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.net.DNS;

import java.net.*;
import java.util.ArrayList;
import java.util.List;

public class HadoopGraphiteUdpTextSink implements org.apache.hadoop.metrics2.MetricsSink {

    private static final Log LOG = LogFactory.getLog(HadoopGraphiteUdpTextSink.class);

    public static final String SERVERS_PROPERTY = "server";
    public static final String SERVERS_PORT_PROPERTY = "port";
    public static final String PREFIX_PROPERTY = "prefix";
    public static final int DEFAULT_PORT = 2003;
    public static final String DEFAULT_HOSTNAME = "#hostname#";
    public static final String DEFAULT_PREFIX = "hadoop";


    private DatagramSocket datagramSocket;
    private InetSocketAddress inetSocketAddress;

    private String hostName;
    private String carbonHost = "localhost";
    private int carbonPort = DEFAULT_PORT;
    private String prefix = DEFAULT_PREFIX;

    private TextMetricVisitor textMetricVisitor = new TextMetricVisitor();

    @Override
    public void putMetrics(MetricsRecord metricsRecord) {
        LOG.debug("Export to " + carbonHost + ":" + carbonPort + " results" + metricsRecord);
        try {
            String[] packets = createTextPacket(metricsRecord);
            for (String p : packets) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Encoded packet : " + p);
                }
                byte[] b = p.getBytes();
                DatagramPacket dp = new DatagramPacket(b, b.length, inetSocketAddress);
                datagramSocket.send(dp);
            }

        } catch (Exception e) {
            LOG.warn("Failure to send result to graphite server '" + carbonHost + ":" + carbonPort, e);
        }
    }

    private String[] createTextPacket(MetricsRecord metricsRecord) {
        List<String> packets = new ArrayList<String>();


        String metricName = metricsRecord.name();
        String contextName = metricsRecord.context();

        // Convert to second
        String time = Long.toString(metricsRecord.timestamp()).substring(0, 10);

        for (AbstractMetric am : metricsRecord.metrics()) {
            StringBuilder sb = new StringBuilder();
            am.visit(textMetricVisitor);
            String value = textMetricVisitor.getValue();
            sb.append(prefix);
            sb.append(".");
            sb.append(hostName.replace('.', '-'));
            sb.append(".");
            sb.append(contextName);
            sb.append(".");
            sb.append(metricName);
            sb.append(".");
            sb.append(am.name().replace(' ', '_'));
            sb.append(" ");
            sb.append(value);
            sb.append(" ");
            sb.append(time);
            packets.add(sb.toString());
        }
        return packets.toArray(new String[packets.size()]);
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
        try {
            datagramSocket = new DatagramSocket();
        } catch (SocketException e) {
            LOG.error("Cannot init datagramsocket. ", e);
        }
    }
}
package jbu.hadoop.graphite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext;
import org.apache.hadoop.metrics.spi.OutputRecord;
import org.apache.hadoop.metrics.spi.Util;
import org.apache.hadoop.net.DNS;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HadoopGraphiteUdpTextContext extends AbstractMetricsContext {

    private static final Log LOG = LogFactory.getLog(HadoopGraphiteUdpTextContext.class);

    public static final String SERVERS_PROPERTY = "server";
    public static final String SERVERS_PORT_PROPERTY = "port";
    public static final String PREFIX_PROPERTY = "prefix";
    public static final int DEFAULT_PORT = 2003;
    public static final String DEFAULT_HOSTNAME = "#hostname#";
    public static final String DEFAULT_PREFIX = "hadoop";


    private String carbonHost;
    private DatagramSocket datagramSocket;
    private InetSocketAddress inetSocketAddress;

    private String hostName;

    private int carbonPort = DEFAULT_PORT;
    private String prefix = DEFAULT_PREFIX;

    private String[] createTextPacket(String contextName, String recordName, OutputRecord metricsRecord) {
        List<String> packets = new ArrayList<String>();

        // Convert to second
        String time = Long.toString(new Date().getTime()).substring(0, 10);

        for (String metricName : metricsRecord.getMetricNames()) {
            StringBuilder sb = new StringBuilder();
            Number value = metricsRecord.getMetric(metricName);
            sb.append(prefix);
            sb.append(".");
            sb.append(hostName.replace('.', '-'));
            sb.append(".");
            sb.append(contextName);
            sb.append(".");
            sb.append(metricName.replace(' ', '_'));
            sb.append(" ");
            sb.append(value);
            sb.append(" ");
            sb.append(time);
            packets.add(sb.toString());
        }
        return packets.toArray(new String[packets.size()]);
    }


    @Override
    protected void emitRecord(String contextName, String recordName, OutputRecord metricsRecord) throws IOException {

        LOG.debug("Export to " + carbonHost + ":" + carbonPort + " results" + metricsRecord);
        try {
            String[] packets = createTextPacket(contextName, recordName, metricsRecord);
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

    @Override
    public void init(String contextName, ContextFactory factory) {
        super.init(contextName, factory);
        LOG.debug("Initializing the Graphite metrics.");

        carbonHost = getAttribute(SERVERS_PROPERTY);
        inetSocketAddress =
                Util.parse(carbonHost, DEFAULT_PORT).get(0);

        try {
            datagramSocket = new DatagramSocket();
        } catch (SocketException se) {
            se.printStackTrace();
        }

        // Take the hostname from the DNS class.
        if (getAttribute("slave.host.name") != null) {
            hostName = getAttribute("slave.host.name");
        } else {
            try {
                hostName = DNS.getDefaultHost(
                        getStringAttributeOrDefault("dfs.datanode.dns.interface", "default"),
                        getStringAttributeOrDefault("dfs.datanode.dns.nameserver", "default"));
            } catch (UnknownHostException uhe) {
                LOG.error(uhe);
                hostName = DEFAULT_HOSTNAME;
            }
        }

        prefix = getStringAttributeOrDefault(PREFIX_PROPERTY, DEFAULT_PREFIX);
    }

    private String getStringAttributeOrDefault(String atttributeKey, String defaultValue) {
        String value;
        if ((value = getAttribute(atttributeKey)) == null) {
            value = defaultValue;
        }
        return value;
    }
}

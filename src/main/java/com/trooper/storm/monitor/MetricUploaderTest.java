package com.trooper.storm.monitor;

import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.TopologyMetricsRunnable;
import com.alibaba.jstorm.daemon.nimbus.metric.uploader.MetricUploader;
import com.alibaba.jstorm.utils.JStormUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Created by lcy on 2016/7/4.
 */
public class MetricUploaderTest implements MetricUploader {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    protected NimbusData nimbusData;
    protected TopologyMetricsRunnable metricsRunnable;
    private static NimbusClient client = null;

    @Override
    public void init(NimbusData nimbusData) throws Exception {
        logger.info("init");
        this.nimbusData = nimbusData;
        this.metricsRunnable = nimbusData.getMetricRunnable();
        ScheduledThreadPoolExecutor scheduledPool = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, "Read-cache-minute");
                thread.setDaemon(true);
                return thread;
            }
        });
        //init a schedule pool to read data from rocksdb of jstorm every 1 min
        scheduledPool.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                getNimbusClient(null);
                if (client == null) {
                    logger.info("nimbus has not started, wait for next schedule");
                    return;
                }
                try {
                    ClusterSummary clusterInfo = client.getClient().getClusterInfo();
                    List<TopologySummary> topologies = clusterInfo.get_topologies();
                    for (TopologySummary topology : topologies) {
                        logger.info("topology info " + topology.get_id() + " " + topology.get_name());
                        TopologyMetric metric = metricsRunnable.getTopologyMetric(topology.get_id());
                        MetricInfo componentMetric = metric.get_componentMetric();
                        Map<String, Map<Integer, MetricSnapshot>> metrics = componentMetric.get_metrics();
                        for (Map.Entry<String, Map<Integer, MetricSnapshot>> oneMetric : metrics.entrySet()) {
                            String[] key = oneMetric.getKey().split("@");
                            String metricKey = key[1] + "@" + key[2] + "@" + key[6];
                            logger.info("metric one minute data for " + metricKey + " " + oneMetric.getValue().get(60));
                        }
                    }

                } catch (TException e) {
                    logger.info("get cluster info error ", e);
                }
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    @Override
    public void cleanup() {
    }

    //copy from  DefaultMetricUploader, useless
    @Override
    public boolean registerMetrics(String clusterName, String topologyId,
                                   Map<String, Long> metrics) {
        logger.info("registerMetrics");
        if (metrics.size() > 0) {
            logger.info("register metrics, topology:{}, total:{}", topologyId, metrics.size());
        }
        return true;
    }

    //copy from DefaultMetricUploader, this method never runs
    @Override
    public boolean upload(String clusterName, String topologyId, TopologyMetric tpMetric, Map<String, Object> metricContext) {
        logger.info("upload1");
        if (tpMetric == null) {
            logger.info("No metric of {}", topologyId);
            return true;
        }

        int totalSize = tpMetric.get_topologyMetric().get_metrics_size() +
                tpMetric.get_componentMetric().get_metrics_size() +
                tpMetric.get_taskMetric().get_metrics_size() +
                tpMetric.get_streamMetric().get_metrics_size() +
                tpMetric.get_workerMetric().get_metrics_size() +
                tpMetric.get_nettyMetric().get_metrics_size();

        logger.info("send metrics, cluster:{}, topology:{}, metric size:{}, metricContext:{}",
                clusterName, topologyId, totalSize, metricContext);

        return true;
    }

    //copy from DefaultMetricUploader, this method will run when the metric change, but still useless for us
    @Override
    public boolean upload(String clusterName, String topologyId, Object key, Map<String, Object> metricContext) {
        metricsRunnable.markUploaded((Integer) key);
        return true;
    }


    @Override
    public boolean sendEvent(String clusterName, TopologyMetricsRunnable.Event event) {
        logger.info("Successfully sendEvent {} of {}", event, clusterName);
        return true;
    }

    //provided by JstormHelper in jstorm 2.2.1
    public NimbusClient getNimbusClient(Map conf) {
        try {
            if (client != null) {
                return client;
            }

            if (conf == null) {
                conf = Utils.readStormConfig();
            }
            client = NimbusClient.getConfiguredClient(conf);
            logger.info("get nimbus client: " + client);
        } catch (Exception e) {
            logger.error("get nimbus client error ", e);
        }
        return client;
    }
}

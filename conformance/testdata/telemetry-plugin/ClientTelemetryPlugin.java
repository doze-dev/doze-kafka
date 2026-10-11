package dozeconformance;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.server.telemetry.ClientTelemetry;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;

/**
 * The smallest plugin that makes an Apache Kafka broker take part in KIP-714: a metrics
 * reporter that also implements ClientTelemetry. A broker advertises GetTelemetrySubscriptions
 * and PushTelemetry (ApiVersions keys 71 and 72) only when one is configured; this one takes
 * the metrics and does nothing with them. It is built and mounted by the conformance recorder
 * (conformance/clients_test.go) and is not part of doze-kafka.
 */
public class ClientTelemetryPlugin implements MetricsReporter, ClientTelemetry {
    @Override
    public void init(List<KafkaMetric> metrics) {}

    @Override
    public void metricChange(KafkaMetric metric) {}

    @Override
    public void metricRemoval(KafkaMetric metric) {}

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}

    @Override
    public ClientTelemetryReceiver clientReceiver() {
        return (context, payload) -> {};
    }
}

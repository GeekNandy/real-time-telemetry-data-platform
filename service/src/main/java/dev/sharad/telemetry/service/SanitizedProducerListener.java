package dev.sharad.telemetry.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.ProducerListener;

/** Producer failures must not copy event keys, contents or upstream exception text into logs. */
final class SanitizedProducerListener<K, V> implements ProducerListener<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(SanitizedProducerListener.class);

  @Override
  public void onError(ProducerRecord<K, V> record, RecordMetadata metadata, Exception failure) {
    LOG.error("Kafka publication failed; delivery remains unconfirmed");
  }
}

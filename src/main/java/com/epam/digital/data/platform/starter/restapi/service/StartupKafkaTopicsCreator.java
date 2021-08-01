package com.epam.digital.data.platform.starter.restapi.service;

import static org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG;

import com.epam.digital.data.platform.starter.restapi.config.properties.KafkaProperties;
import com.epam.digital.data.platform.starter.restapi.config.properties.KafkaProperties.Handler;
import com.epam.digital.data.platform.starter.restapi.exception.CreateKafkaTopicException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.stereotype.Component;

@Component
public class StartupKafkaTopicsCreator {

  private static final long DAYS_TO_MS = 24 * 60 * 60 * 1000L;
  private static final String READ = "read";
  private static final String SEARCH = "search";

  private final AdminClient adminClient;
  private final KafkaProperties kafkaProperties;

  public StartupKafkaTopicsCreator(AdminClient adminClient,
      KafkaProperties kafkaProperties) {
    this.adminClient = adminClient;
    this.kafkaProperties = kafkaProperties;
  }

  @PostConstruct
  public void createKafkaTopics() {
    long maxElapsedTime = kafkaProperties.getErrorHandler().getMaxElapsedTime();

    Set<String> missingTopicNames = getMissingTopicNames(maxElapsedTime);

    var createTopicsResult = adminClient.createTopics(getNewTopics(missingTopicNames));
    try {
      createTopicsResult.all().get(maxElapsedTime, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new CreateKafkaTopicException("Failed to create a kafka topic: ", e);
    }
  }

  private Set<String> getMissingTopicNames(long maxElapsedTime) {
    Set<String> existingTopics;
    try {
      existingTopics = adminClient.listTopics().names().get(maxElapsedTime, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new CreateKafkaTopicException("Failed to retrieve existing kafka topics: ", e);
    }
    Set<String> requiredTopics = getRequiredTopics(kafkaProperties.getTopics());
    requiredTopics.removeAll(existingTopics);
    return requiredTopics;
  }

  private Set<String> getRequiredTopics(Map<String, Handler> rootsOfTopicNames) {
    Set<String> requiredTopics = new HashSet<>();
    for (Entry<String, Handler> e : rootsOfTopicNames.entrySet()) {
      requiredTopics.add(e.getValue().getRequest());
      requiredTopics.add(e.getValue().getRequest() + ".DLT");
      requiredTopics.add(e.getValue().getReplay());
    }
    return requiredTopics;
  }

  private Collection<NewTopic> getNewTopics(Set<String> requiredTopics) {
    return requiredTopics.stream()
        .map(topicName -> new NewTopic(topicName,
            kafkaProperties.getTopicProperties().getNumPartitions(),
            kafkaProperties.getTopicProperties().getReplicationFactor()))
        .map(topic -> topic.configs(getRetentionPolicy(topic.name())))
        .collect(Collectors.toSet());
  }

  private Map<String, String> getRetentionPolicy(String topicName) {

    var retentionPolicyInDays = kafkaProperties.getTopicProperties().getRetentionPolicyInDays();

    int days = retentionPolicyInDays.getWrite();
    if (topicName.startsWith(READ) || topicName.startsWith(SEARCH)) {
      days = retentionPolicyInDays.getRead();
    }

    return Map.of(RETENTION_MS_CONFIG, Long.toString(days * DAYS_TO_MS));
  }
}

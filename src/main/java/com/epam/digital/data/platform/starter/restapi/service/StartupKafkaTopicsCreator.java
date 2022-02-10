/*
 * Copyright 2021 EPAM Systems.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class StartupKafkaTopicsCreator {

  private static final long DAYS_TO_MS = 24 * 60 * 60 * 1000L;
  private static final Long TOPIC_CREATION_TIMEOUT = 60L;

  private static final String READ = "read";
  private static final String SEARCH = "search";

  private final Logger log = LoggerFactory.getLogger(StartupKafkaTopicsCreator.class);

  private final Supplier<AdminClient> adminClientFactory;
  private final KafkaProperties kafkaProperties;

  public StartupKafkaTopicsCreator(
          Supplier<AdminClient> adminClientFactory, KafkaProperties kafkaProperties) {
    this.adminClientFactory = adminClientFactory;
    this.kafkaProperties = kafkaProperties;
  }

  @PostConstruct
  public void createKafkaTopics() {
    try (var kafkaAdminClient = adminClientFactory.get()) {
      var missingTopicNames = getMissingTopicNames(kafkaAdminClient);
      createTopics(missingTopicNames, kafkaAdminClient);
    }
  }

  private Set<String> getMissingTopicNames(AdminClient kafkaAdminClient) {
    Set<String> existingTopics;
    try {
      existingTopics = kafkaAdminClient.listTopics().names()
          .get(TOPIC_CREATION_TIMEOUT, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new CreateKafkaTopicException(String.format(
          "Failed to retrieve existing kafka topics in %d sec", TOPIC_CREATION_TIMEOUT), e);
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

  private void createTopics(Set<String> topicNames, AdminClient kafkaAdminClient) {
    var createTopicsResult = kafkaAdminClient.createTopics(getNewTopics(topicNames));
    createTopicsResult.values().forEach(this::handleTopicCreationResult);
  }

  private void handleTopicCreationResult(String topicName, KafkaFuture<Void> future) {
    try {
      future.get(TOPIC_CREATION_TIMEOUT, TimeUnit.SECONDS);
    } catch (Exception e) {
      if (e.getCause() instanceof TopicExistsException) {
        log.warn("Topic {} was in missing topics list, but now exists", topicName);
      } else {
        throw new CreateKafkaTopicException(
            String.format("Failed to create topic %s in %d sec", topicName, TOPIC_CREATION_TIMEOUT),
            e);
      }
    }
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

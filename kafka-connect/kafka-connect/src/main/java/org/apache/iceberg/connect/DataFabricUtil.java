/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.connect;

import com.mapr.kafka.eventstreams.Streams;
import com.mapr.kafka.eventstreams.impl.admin.AssignInfo;
import com.mapr.kafka.eventstreams.impl.admin.MarlinAdminImpl;
import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.kafka.common.TopicPartition;

public class DataFabricUtil {

  private DataFabricUtil() {}

  public static Collection<String> partitions2Streams(Collection<TopicPartition> partitions) {
    return partitions.stream()
        .map(tp -> Iterables.get(Splitter.on(':').split(tp.topic()), 0))
        .collect(Collectors.toUnmodifiableSet());
  }

  public static Collection<TopicPartition> getGroupAssignments(
      Collection<String> streams, String groupId) {
    Collection<TopicPartition> topicPartitions = Sets.newHashSet();
    try (MarlinAdminImpl admin = (MarlinAdminImpl) Streams.newAdmin(new Configuration())) {
      for (String stream : streams) {
        Collection<AssignInfo> assignments = admin.listAssigns(stream, groupId, null);
        for (AssignInfo assignment : assignments) {
          String topic = String.format("%s:%s", assignment.streamName(), assignment.topic());
          Collection<Integer> partitions = Sets.newHashSet();
          for (int i = 0; i < assignment.numListeners(); i++) {
            partitions.addAll(assignment.listenerAssignment(i));
          }
          partitions.forEach(p -> topicPartitions.add(new TopicPartition(topic, p)));
        }
      }
    } catch (IOException e) {
      throw new IllegalStateException("Could not get group assignments", e);
    }
    return topicPartitions;
  }
}

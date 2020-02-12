/*
 * Copyright (c) SiteWhere, LLC. All rights reserved. http://www.sitewhere.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.sitewhere.nifi.processors;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.nifi.logging.ComponentLog;

import com.sitewhere.nifi.controllers.SiteWhereContextService;

/**
 * Base class for components which need to verify a Kafka topic exists or create
 * one if not.
 */
public class KafkaTopicWaiter implements Runnable {

    /** Kafka availability check interval */
    private static final int KAFKA_RETRY_INTERVAL_MS = 5 * 1000;

    /** SiteWhere context service */
    private SiteWhereContextService contextService;

    /** Topic to be verified/created */
    private String topicName;

    /** Nifi logger */
    private ComponentLog logger;

    /** Kafka admin client */
    private AdminClient kafkaAdmin;

    public KafkaTopicWaiter(SiteWhereContextService contextService, String topicName, ComponentLog logger) {
	this.contextService = contextService;
	this.topicName = topicName;
	this.logger = logger;
    }

    /**
     * Build configuration settings used by admin client.
     * 
     * @return
     */
    protected Properties buildAdminConfiguration() {
	Properties config = new Properties();
	config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getContextService().getKafkaBootstrapServers());
	return config;
    }

    /*
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
	getLogger().info("Attempting to connect to Kafka...");
	while (true) {
	    try {
		KafkaTopicWaiter.this.kafkaAdmin = AdminClient.create(buildAdminConfiguration());
		Map<String, TopicDescription> topicMap = getKafkaAdmin().describeTopics(Arrays.asList(getTopicName()))
			.all().get();
		TopicDescription topic = topicMap.get(getTopicName());
		if (topic != null) {
		    getLogger().info(String.format("Kafka topic '%s' detected as available.", getTopicName()));
		    return;
		}
	    } catch (ExecutionException e) {
		Throwable t = e.getCause();
		if (t instanceof UnknownTopicOrPartitionException) {
		    getLogger().info(String.format("Kafka topic '%s' does not exist. Waiting for it to be created.",
			    getTopicName()));
		} else {
		    getLogger().warn("Execution exception connecting to Kafka. Will continue attempting to connect. ("
			    + e.getMessage() + ")", t);
		}
	    } catch (ConfigException e) {
		getLogger().warn("Configuration issue connecting to Kafka. Will continue attempting to connect.", e);
	    } catch (Throwable t) {
		getLogger().warn("Exception while connecting to Kafka. Will continue attempting to connect.", t);
	    }
	    try {
		Thread.sleep(KAFKA_RETRY_INTERVAL_MS);
	    } catch (InterruptedException e) {
		getLogger().warn("Interrupted while waiting for Kafka to become available.");
		return;
	    }
	}
    }

    protected ComponentLog getLogger() {
	return logger;
    }

    protected SiteWhereContextService getContextService() {
	return contextService;
    }

    protected String getTopicName() {
	return topicName;
    }

    protected AdminClient getKafkaAdmin() {
	return kafkaAdmin;
    }
}

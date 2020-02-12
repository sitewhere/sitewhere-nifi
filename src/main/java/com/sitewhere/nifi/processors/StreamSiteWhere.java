/*
 * SiteWhere LLC licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sitewhere.nifi.processors;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;

import com.sitewhere.nifi.controllers.SiteWhereContextService;

@CapabilityDescription("Consumes SiteWhere events from well-known topics using an Apache Kafka 2.0 consumer.")
@Tags({ "SiteWhere", "Stream", "Kafka", "Get", "Ingest", "Ingress", "Topic", "Consume", "2.0" })
public class StreamSiteWhere extends AbstractProcessor {

    /** Constant for SiteWhere product name */
    private static final String SITEWHERE_PRODUCT = "sitewhere";

    /** SiteWhere context service */
    public static final PropertyDescriptor SITEWHERE_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
	    .name("SiteWhere Context Service")
	    .description("Service which provides context such as instance id and tenant id for processors.")
	    .required(true).identifiesControllerService(SiteWhereContextService.class).build();

    /** Value for stream from event sources decoded events */
    public static final AllowableValue STREAM_EVENT_SOURCES_DECODED_EVENTS = new AllowableValue(
	    "event-sources-decoded-events", "Event Sources - Decoded Events",
	    "Events from event sources after payloads have been decoded.");

    /** Choice of which stream to consume */
    public static final PropertyDescriptor SITEWHERE_STREAM = new PropertyDescriptor.Builder().name("sitewhere-stream")
	    .displayName("SiteWhere Stream")
	    .description("Chooses the SiteWhere event stream which will act as the source of events.").required(true)
	    .allowableValues(STREAM_EVENT_SOURCES_DECODED_EVENTS)
	    .defaultValue(STREAM_EVENT_SOURCES_DECODED_EVENTS.getValue()).build();

    /** Success relationship */
    public static final Relationship SUCCESS = new Relationship.Builder().name("success")
	    .description("Successfully pulled event from SiteWhere").build();

    /** List of property descriptors */
    private List<PropertyDescriptor> descriptors;

    /** Set of relationships */
    private Set<Relationship> relationships;

    /** Pipeline instance */
    private KafkaStreams pipeline;

    /** Handles streaming events */
    private StreamProcessorSupplier supplier;

    /** Flag for pipeline starting indicator */
    private AtomicBoolean starting = new AtomicBoolean();

    /** Flag for pipeline started indicator */
    private AtomicBoolean started = new AtomicBoolean();

    /*
     * @see
     * org.apache.nifi.processor.AbstractSessionFactoryProcessor#init(org.apache.
     * nifi.processor.ProcessorInitializationContext)
     */
    @Override
    protected void init(ProcessorInitializationContext context) {
	descriptors = new ArrayList<>();
	descriptors.add(SITEWHERE_CONTEXT_SERVICE);
	descriptors.add(SITEWHERE_STREAM);
	descriptors = Collections.unmodifiableList(descriptors);

	relationships = new HashSet<>();
	relationships.add(SUCCESS);
	relationships = Collections.unmodifiableSet(relationships);

	// Initialize state.
	getStarting().set(false);
	getStarted().set(false);
    }

    /**
     * Prepare the processing pipeline.
     * 
     * @param context
     * @param session
     * @return
     * @throws ProcessException
     */
    protected KafkaStreams preparePipeline(ProcessContext context, ProcessSession session) throws ProcessException {
	SiteWhereContextService contextService = context.getProperty(SITEWHERE_CONTEXT_SERVICE)
		.asControllerService(SiteWhereContextService.class);

	String topicChoice = context.getProperty(SITEWHERE_STREAM).getValue();
	String topic = StreamConstants.getTopic(contextService, topicChoice);
	if (topic == null) {
	    String message = "No stream chosen for SiteWhere stream processor.";
	    getLogger().error(message);
	    throw new ProcessException(message);
	}
	getLogger().info(String.format("Preparing Kafka Streams pipeline for handling '%s'...", topic));
	new KafkaTopicWaiter(contextService, topic, getLogger()).run();

	Properties props = new Properties();
	String appId = String.format("%s-%s-%s-%s", SITEWHERE_PRODUCT, contextService.getInstanceId(),
		contextService.getTenantId(), getIdentifier());
	props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
	props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, contextService.getKafkaBootstrapServers());
	props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
	props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());

	final StreamsBuilder builder = new StreamsBuilder();
	this.supplier = new StreamProcessorSupplier();
	builder.stream(topic, Consumed.with(Serdes.String(), Serdes.ByteArray())).process(getSupplier(), new String[0]);

	return new KafkaStreams(builder.build(), props);
    }

    /*
     * @see org.apache.nifi.processor.AbstractProcessor#onTrigger(org.apache.nifi.
     * processor.ProcessContext, org.apache.nifi.processor.ProcessSession)
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
	if (!getStarting().get()) {
	    if (getPipeline() == null && getStarting().compareAndSet(false, true)) {
		this.pipeline = preparePipeline(context, session);
		getPipeline().start();

		State state = getPipeline().state();
		if (state == State.ERROR) {
		    throw new ProcessException("Kafka Streams pipeline is in an error state.");
		}

		getStarting().set(false);
		getStarted().set(true);
	    }
	}
	if (getStarted().get()) {
	    processQueuedEvents(context, session);
	    context.yield();
	}
    }

    /**
     * Process queued events until none are available. Push binary event content
     * into {@link FlowFile}s.
     * 
     * @param context
     * @param session
     */
    protected void processQueuedEvents(ProcessContext context, ProcessSession session) {
	try {
	    long counter = 0;
	    while (true) {
		BinaryPayload event = getSupplier().getQueue().poll(100, TimeUnit.MILLISECONDS);
		if (event != null) {
		    FlowFile flowfile = session.create();
		    session.write(flowfile, new OutputStreamCallback() {

			/*
			 * @see org.apache.nifi.processor.io.OutputStreamCallback#process(java.io.
			 * OutputStream)
			 */
			@Override
			public void process(OutputStream out) throws IOException {
			    out.write(event.getPayload());
			}
		    });
		    session.getProvenanceReporter().create(flowfile);

		    session.transfer(flowfile, StreamSiteWhere.SUCCESS);
		    session.commit();
		    counter++;
		} else {
		    getLogger().info(String.format("Processed %s events in thread '%s'.", String.valueOf(counter),
			    Thread.currentThread().getName()));
		    return;
		}
	    }
	} catch (InterruptedException e) {
	    return;
	}
    }

    /**
     * Cleanly handle component being unscheduled.
     * 
     * @param context
     */
    @OnUnscheduled
    public void onUnscheduled(ProcessContext context) {
	if (getPipeline() != null) {
	    getPipeline().close();
	    this.pipeline = null;
	}
    }

    /*
     * @see
     * org.apache.nifi.processor.AbstractSessionFactoryProcessor#getRelationships()
     */
    @Override
    public Set<Relationship> getRelationships() {
	return relationships;
    }

    /*
     * @see org.apache.nifi.components.AbstractConfigurableComponent#
     * getSupportedPropertyDescriptors()
     */
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
	return descriptors;
    }

    protected KafkaStreams getPipeline() {
	return pipeline;
    }

    protected StreamProcessorSupplier getSupplier() {
	return supplier;
    }

    protected AtomicBoolean getStarting() {
	return starting;
    }

    protected AtomicBoolean getStarted() {
	return started;
    }
}

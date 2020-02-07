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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sitewhere.nifi.controllers.SiteWhereContextService;

@CapabilityDescription("Consumes SiteWhere events from well-known topics using an Apache Kafka 2.0 consumer.")
@Tags({ "SiteWhere", "Stream", "Kafka", "Get", "Ingest", "Ingress", "Topic", "Consume", "2.0" })
@WritesAttributes({
	@WritesAttribute(attribute = KafkaProcessorUtils.KAFKA_COUNT, description = "The number of messages written if more than one"),
	@WritesAttribute(attribute = KafkaProcessorUtils.KAFKA_KEY, description = "The key of message if present and if single message. "
		+ "How the key is encoded depends on the value of the 'Key Attribute Encoding' property."),
	@WritesAttribute(attribute = KafkaProcessorUtils.KAFKA_OFFSET, description = "The offset of the message in the partition of the topic."),
	@WritesAttribute(attribute = KafkaProcessorUtils.KAFKA_TIMESTAMP, description = "The timestamp of the message in the partition of the topic."),
	@WritesAttribute(attribute = KafkaProcessorUtils.KAFKA_PARTITION, description = "The partition of the topic the message or message bundle is from"),
	@WritesAttribute(attribute = KafkaProcessorUtils.KAFKA_TOPIC, description = "The topic the message or message bundle is from") })
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@DynamicProperty(name = "The name of a Kafka configuration property.", value = "The value of a given Kafka configuration property.", description = "These properties will be added on the Kafka configuration after loading any provided configuration properties."
	+ " In the event a dynamic property represents a property that was already set, its value will be ignored and WARN message logged."
	+ " For the list of available Kafka properties please refer to: http://kafka.apache.org/documentation.html#configuration. ", expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY)
public class SiteWhereEvents extends AbstractProcessor {

    /** Static logger instance */
    @SuppressWarnings("unused")
    private static Logger LOGGER = LoggerFactory.getLogger(SiteWhereEvents.class);

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

    public static final AllowableValue OFFSET_EARLIEST = new AllowableValue("earliest", "earliest",
	    "Automatically reset the offset to the earliest offset");

    public static final AllowableValue OFFSET_LATEST = new AllowableValue("latest", "latest",
	    "Automatically reset the offset to the latest offset");

    public static final AllowableValue OFFSET_NONE = new AllowableValue("none", "none",
	    "Throw exception to the consumer if no previous offset is found for the consumer's group");

    public static final PropertyDescriptor AUTO_OFFSET_RESET = new PropertyDescriptor.Builder()
	    .name(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).displayName("Offset Reset")
	    .description(
		    "Allows you to manage the condition when there is no initial offset in Kafka or if the current offset does not exist any "
			    + "more on the server (e.g. because that data has been deleted). Corresponds to Kafka's 'auto.offset.reset' property.")
	    .required(true).allowableValues(OFFSET_EARLIEST, OFFSET_LATEST, OFFSET_NONE)
	    .defaultValue(OFFSET_LATEST.getValue()).build();

    public static final PropertyDescriptor KEY_ATTRIBUTE_ENCODING = new PropertyDescriptor.Builder()
	    .name("key-attribute-encoding").displayName("Key Attribute Encoding")
	    .description("FlowFiles that are emitted have an attribute named '" + KafkaProcessorUtils.KAFKA_KEY
		    + "'. This property dictates how the value of the attribute should be encoded.")
	    .required(true).defaultValue(KafkaProcessorUtils.UTF8_ENCODING.getValue())
	    .allowableValues(KafkaProcessorUtils.UTF8_ENCODING, KafkaProcessorUtils.HEX_ENCODING).build();

    public static final PropertyDescriptor MESSAGE_DEMARCATOR = new PropertyDescriptor.Builder()
	    .name("message-demarcator").displayName("Message Demarcator").required(false).addValidator(Validator.VALID)
	    .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
	    .description(
		    "Since KafkaConsumer receives messages in batches, you have an option to output FlowFiles which contains "
			    + "all Kafka messages in a single batch for a given topic and partition and this property allows you to provide a string (interpreted as UTF-8) to use "
			    + "for demarcating apart multiple Kafka messages. This is an optional property and if not provided each Kafka message received "
			    + "will result in a single FlowFile which  "
			    + "time it is triggered. To enter special character such as 'new line' use CTRL+Enter or Shift+Enter depending on the OS")
	    .build();

    public static final PropertyDescriptor HEADER_NAME_REGEX = new PropertyDescriptor.Builder()
	    .name("header-name-regex").displayName("Headers to Add as Attributes (Regex)")
	    .description("A Regular Expression that is matched against all message headers. "
		    + "Any message header whose name matches the regex will be added to the FlowFile as an Attribute. "
		    + "If not specified, no Header values will be added as FlowFile attributes. If two messages have a different value for the same header and that header is selected by "
		    + "the provided regex, then those two messages must be added to different FlowFiles. As a result, users should be cautious about using a regex like "
		    + "\".*\" if messages are expected to have header values that are unique per message, such as an identifier or timestamp, because it will prevent NiFi from bundling "
		    + "the messages together efficiently.")
	    .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
	    .expressionLanguageSupported(ExpressionLanguageScope.NONE).required(false).build();

    public static final PropertyDescriptor MAX_POLL_RECORDS = new PropertyDescriptor.Builder().name("max.poll.records")
	    .displayName("Max Poll Records")
	    .description("Specifies the maximum number of records Kafka should return in a single poll.")
	    .required(false).defaultValue("10000").addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();

    public static final PropertyDescriptor MAX_UNCOMMITTED_TIME = new PropertyDescriptor.Builder()
	    .name("max-uncommit-offset-wait").displayName("Max Uncommitted Time")
	    .description("Specifies the maximum amount of time allowed to pass before offsets must be committed. "
		    + "This value impacts how often offsets will be committed.  Committing offsets less often increases "
		    + "throughput but also increases the window of potential data duplication in the event of a rebalance "
		    + "or JVM restart between commits.  This value is also related to maximum poll records and the use "
		    + "of a message demarcator.  When using a message demarcator we can have far more uncommitted messages "
		    + "than when we're not as there is much less for us to keep track of in memory.")
	    .required(false).defaultValue("1 secs").addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).build();

    public static final PropertyDescriptor COMMS_TIMEOUT = new PropertyDescriptor.Builder()
	    .name("Communications Timeout").displayName("Communications Timeout")
	    .description("Specifies the timeout that the consumer should use when communicating with the Kafka Broker")
	    .required(true).defaultValue("60 secs").addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).build();

    public static final PropertyDescriptor HONOR_TRANSACTIONS = new PropertyDescriptor.Builder()
	    .name("honor-transactions").displayName("Honor Transactions")
	    .description(
		    "Specifies whether or not NiFi should honor transactional guarantees when communicating with Kafka. If false, the Processor will use an \"isolation level\" of "
			    + "read_uncomitted. This means that messages will be received as soon as they are written to Kafka but will be pulled, even if the producer cancels the transactions. If "
			    + "this value is true, NiFi will not receive any messages for which the producer's transaction was canceled, but this can result in some latency since the consumer must wait "
			    + "for the producer to finish its entire transaction instead of pulling as the messages become available.")
	    .expressionLanguageSupported(ExpressionLanguageScope.NONE).allowableValues("true", "false")
	    .defaultValue("true").required(true).build();

    public static final PropertyDescriptor MESSAGE_HEADER_ENCODING = new PropertyDescriptor.Builder()
	    .name("message-header-encoding").displayName("Message Header Encoding")
	    .description(
		    "Any message header that is found on a Kafka message will be added to the outbound FlowFile as an attribute. "
			    + "This property indicates the Character Encoding to use for deserializing the headers.")
	    .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR).defaultValue("UTF-8").required(false).build();

    static {
	List<PropertyDescriptor> descriptors = new ArrayList<>();
	descriptors.addAll(KafkaProcessorUtils.getCommonPropertyDescriptors());
	descriptors.add(SITEWHERE_CONTEXT_SERVICE);
	descriptors.add(SITEWHERE_STREAM);
	descriptors.add(HONOR_TRANSACTIONS);
	descriptors.add(AUTO_OFFSET_RESET);
	descriptors.add(KEY_ATTRIBUTE_ENCODING);
	descriptors.add(MESSAGE_DEMARCATOR);
	descriptors.add(MESSAGE_HEADER_ENCODING);
	descriptors.add(HEADER_NAME_REGEX);
	descriptors.add(MAX_POLL_RECORDS);
	descriptors.add(MAX_UNCOMMITTED_TIME);
	descriptors.add(COMMS_TIMEOUT);
	DESCRIPTORS = Collections.unmodifiableList(descriptors);
    }

    static final List<PropertyDescriptor> DESCRIPTORS;

    private SiteWhereKafka kafka;

    public SiteWhereEvents() {
	this.kafka = new SiteWhereKafka();
    }

    /*
     * @see org.apache.nifi.processor.AbstractProcessor#onTrigger(org.apache.nifi.
     * processor.ProcessContext, org.apache.nifi.processor.ProcessSession)
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
	getKafka().onTrigger(context, session);
    }

    /*
     * @see
     * org.apache.nifi.processor.AbstractSessionFactoryProcessor#getRelationships()
     */
    @Override
    public Set<Relationship> getRelationships() {
	return getKafka().getRelationships();
    }

    /*
     * @see org.apache.nifi.components.AbstractConfigurableComponent#
     * getSupportedPropertyDescriptors()
     */
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
	return DESCRIPTORS;
    }

    /*
     * @see org.apache.nifi.components.AbstractConfigurableComponent#
     * getSupportedDynamicPropertyDescriptor(java.lang.String)
     */
    @Override
    public PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
	return getKafka().getSupportedDynamicPropertyDescriptor(propertyDescriptorName);
    }

    /*
     * @see
     * org.apache.nifi.components.AbstractConfigurableComponent#customValidate(org.
     * apache.nifi.components.ValidationContext)
     */
    @Override
    public Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
	return getKafka().customValidate(validationContext);
    }

    @OnUnscheduled
    public void interruptActiveThreads() {
	getKafka().interruptActiveThreads();
    }

    @OnStopped
    public void close() {
	getKafka().close();
    }

    protected SiteWhereKafka getKafka() {
	return kafka;
    }

}

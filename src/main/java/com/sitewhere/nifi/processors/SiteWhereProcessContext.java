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

import org.apache.commons.text.StringSubstitutor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.processor.ProcessContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sitewhere.nifi.controllers.SiteWhereContextService;

/**
 * Wraps the standard {@link ProcessContext} so that values required by Kafka
 * processor are available.
 */
public class SiteWhereProcessContext extends ProcessContextFacade {

    /** Static logger instance */
    private static Logger LOGGER = LoggerFactory.getLogger(SiteWhereEvents.class);

    /** Topic with instance id, tenant id, etc substituted */
    private String topicWithSubstitutions;

    public SiteWhereProcessContext(ProcessContext wrapped) {
	super(wrapped);
	this.topicWithSubstitutions = getTopicWithSubstitutions();
	LOGGER.info(String.format("Will be reading events from '%s'", topicWithSubstitutions));
    }

    /**
     * Get topic expression for the selected stream.
     * 
     * @return
     */
    protected String getTopicForChosenStream() {
	PropertyValue property = getProperty(SiteWhereEvents.SITEWHERE_STREAM);
	String value = property != null ? property.getValue() : null;
	return value != null ? SiteWhereStreams.getTopic(value) : null;
    }

    /**
     * Get topic with placeholders substituted based on controller service data.
     * 
     * @return
     */
    protected String getTopicWithSubstitutions() {
	String topic = getTopicForChosenStream();
	if (topic != null) {
	    SiteWhereContextService sitewhere = getProperty(SiteWhereEvents.SITEWHERE_CONTEXT_SERVICE)
		    .asControllerService(SiteWhereContextService.class);
	    StringSubstitutor sub = new StringSubstitutor(new SiteWhereStringLookup(sitewhere));
	    return sub.replace(topic);
	}
	return null;
    }

    /*
     * @see
     * com.sitewhere.nifi.processors.ProcessContextFacade#getProperty(org.apache.
     * nifi.components.PropertyDescriptor)
     */
    @Override
    public PropertyValue getProperty(PropertyDescriptor descriptor) {
	if (descriptor.getName().equals("topic")) {
	    LOGGER.info(String.format("Using topic %s.", topicWithSubstitutions));
	    return getWrapped().newPropertyValue(topicWithSubstitutions);
	} else if (descriptor.getName().equals("topic_type")) {
	    return getWrapped().newPropertyValue("names");
	} else if (descriptor.getName().equals(ConsumerConfig.GROUP_ID_CONFIG)) {
	    LOGGER.info(String.format("Using topic %s.", topicWithSubstitutions + ".nifi"));
	    return getWrapped().newPropertyValue(topicWithSubstitutions + ".nifi");
	}
	return super.getProperty(descriptor);
    }
}

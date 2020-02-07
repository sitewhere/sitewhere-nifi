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

import java.util.Collection;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_0;

/**
 * Wraps {@link ConsumeKafka_2_0} to make protected methods public so class can
 * be composed.
 */
public class SiteWhereKafka extends ConsumeKafka_2_0 {

    /*
     * @see org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_0#
     * getSupportedPropertyDescriptors()
     */
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
	return super.getSupportedPropertyDescriptors();
    }

    /*
     * @see org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_0#
     * getSupportedDynamicPropertyDescriptor(java.lang.String)
     */
    @Override
    public PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
	return super.getSupportedDynamicPropertyDescriptor(propertyDescriptorName);
    }

    /*
     * @see
     * org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_0#customValidate(org.
     * apache.nifi.components.ValidationContext)
     */
    @Override
    public Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
	return super.customValidate(validationContext);
    }

    /*
     * @see
     * org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_0#onTrigger(org.apache
     * .nifi.processor.ProcessContext, org.apache.nifi.processor.ProcessSession)
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
	super.onTrigger(new SiteWhereProcessContext(context), session);
    }

    protected class SiteWhereProcessContext extends ProcessContextFacade {

	public SiteWhereProcessContext(ProcessContext wrapped) {
	    super(wrapped);
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

	/*
	 * @see
	 * com.sitewhere.nifi.processors.ProcessContextFacade#getProperty(org.apache.
	 * nifi.components.PropertyDescriptor)
	 */
	@Override
	public PropertyValue getProperty(PropertyDescriptor descriptor) {
	    if (descriptor.getName().equals("topic")) {
		return getWrapped().newPropertyValue(getTopicForChosenStream());
	    } else if (descriptor.getName().equals("topic_type")) {
		return getWrapped().newPropertyValue("names");
	    } else if (descriptor.getName().equals(ConsumerConfig.GROUP_ID_CONFIG)) {
		return getWrapped().newPropertyValue(getTopicForChosenStream() + ".nifi");
	    }
	    return super.getProperty(descriptor);
	}
    }
}

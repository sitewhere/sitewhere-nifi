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

import java.util.Map;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.scheduling.ExecutionNode;

/**
 * Provides a facade which wraps a {@link ProcessContext} so that individual
 * methods may be overridden.
 */
public class ProcessContextFacade implements ProcessContext {

    /** Wrapped context */
    private ProcessContext wrapped;

    public ProcessContextFacade(ProcessContext wrapped) {
	this.wrapped = wrapped;
    }

    /*
     * @see org.apache.nifi.context.PropertyContext#getProperty(org.apache.nifi.
     * components.PropertyDescriptor)
     */
    @Override
    public PropertyValue getProperty(PropertyDescriptor descriptor) {
	return getWrapped().getProperty(descriptor);
    }

    /*
     * @see org.apache.nifi.context.PropertyContext#getAllProperties()
     */
    @Override
    public Map<String, String> getAllProperties() {
	return getWrapped().getAllProperties();
    }

    /*
     * @see org.apache.nifi.processor.ProcessContext#getProperty(java.lang.String)
     */
    @Override
    public PropertyValue getProperty(String propertyName) {
	return getWrapped().getProperty(propertyName);
    }

    /*
     * @see
     * org.apache.nifi.processor.ProcessContext#newPropertyValue(java.lang.String)
     */
    @Override
    public PropertyValue newPropertyValue(String rawValue) {
	return getWrapped().newPropertyValue(rawValue);
    }

    /*
     * @see org.apache.nifi.processor.ProcessContext#yield()
     */
    @Override
    public void yield() {
	getWrapped().yield();
    }

    /*
     * @see org.apache.nifi.processor.ProcessContext#getMaxConcurrentTasks()
     */
    @Override
    public int getMaxConcurrentTasks() {
	return getWrapped().getMaxConcurrentTasks();
    }

    /*
     * @see org.apache.nifi.processor.ProcessContext#getExecutionNode()
     */
    @Override
    public ExecutionNode getExecutionNode() {
	return getWrapped().getExecutionNode();
    }

    /*
     * @see org.apache.nifi.processor.ProcessContext#getAnnotationData()
     */
    @Override
    public String getAnnotationData() {
	return getWrapped().getAnnotationData();
    }

    /*
     * @see org.apache.nifi.processor.ProcessContext#getProperties()
     */
    @Override
    public Map<PropertyDescriptor, String> getProperties() {
	return getWrapped().getProperties();
    }

    /*
     * @see org.apache.nifi.processor.ProcessContext#encrypt(java.lang.String)
     */
    @Override
    public String encrypt(String unencrypted) {
	return getWrapped().encrypt(unencrypted);
    }

    /*
     * @see org.apache.nifi.processor.ProcessContext#decrypt(java.lang.String)
     */
    @Override
    public String decrypt(String encrypted) {
	return getWrapped().decrypt(encrypted);
    }

    /*
     * @see org.apache.nifi.processor.ProcessContext#getControllerServiceLookup()
     */
    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
	return getWrapped().getControllerServiceLookup();
    }

    /*
     * @see org.apache.nifi.processor.ProcessContext#getAvailableRelationships()
     */
    @Override
    public Set<Relationship> getAvailableRelationships() {
	return getWrapped().getAvailableRelationships();
    }

    /*
     * @see org.apache.nifi.processor.ProcessContext#hasIncomingConnection()
     */
    @Override
    public boolean hasIncomingConnection() {
	return getWrapped().hasIncomingConnection();
    }

    /*
     * @see org.apache.nifi.processor.ProcessContext#hasNonLoopConnection()
     */
    @Override
    public boolean hasNonLoopConnection() {
	return getWrapped().hasNonLoopConnection();
    }

    /*
     * @see org.apache.nifi.processor.ProcessContext#hasConnection(org.apache.nifi.
     * processor.Relationship)
     */
    @Override
    public boolean hasConnection(Relationship relationship) {
	return getWrapped().hasConnection(relationship);
    }

    /*
     * @see
     * org.apache.nifi.processor.ProcessContext#isExpressionLanguagePresent(org.
     * apache.nifi.components.PropertyDescriptor)
     */
    @Override
    public boolean isExpressionLanguagePresent(PropertyDescriptor property) {
	return getWrapped().isExpressionLanguagePresent(property);
    }

    /*
     * @see org.apache.nifi.processor.ProcessContext#getStateManager()
     */
    @Override
    public StateManager getStateManager() {
	return getWrapped().getStateManager();
    }

    /*
     * @see org.apache.nifi.processor.ProcessContext#getName()
     */
    @Override
    public String getName() {
	return getWrapped().getName();
    }

    protected ProcessContext getWrapped() {
	return wrapped;
    }
}

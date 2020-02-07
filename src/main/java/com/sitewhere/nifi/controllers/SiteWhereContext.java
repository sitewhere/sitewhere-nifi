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
package com.sitewhere.nifi.controllers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * Implementation of {@link SiteWhereContextService} which provides context to
 * SiteWhere Nifi processors.
 */
@Tags({ "sitewhere", "context" })
@CapabilityDescription("Provides instance/tenant context for SiteWhere processors.")
public class SiteWhereContext extends AbstractControllerService implements SiteWhereContextService {

    /** SiteWhere instance id */
    public static final PropertyDescriptor INSTANCE_ID = new PropertyDescriptor.Builder().name("instance-id")
	    .displayName("Instance Id").description("SiteWhere instance id.").defaultValue("sitewhere1")
	    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    /** SiteWhere tenant id */
    public static final PropertyDescriptor TENANT_ID = new PropertyDescriptor.Builder().name("tenant-id")
	    .displayName("Tenant Id").description("SiteWhere tenant id.").defaultValue("default")
	    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    /** List of service properties */
    private static final List<PropertyDescriptor> SERVICE_PROPERTIES;

    static {
	final List<PropertyDescriptor> props = new ArrayList<>();
	props.add(INSTANCE_ID);
	props.add(TENANT_ID);
	SERVICE_PROPERTIES = Collections.unmodifiableList(props);
    }

    /*
     * @see org.apache.nifi.components.AbstractConfigurableComponent#
     * getSupportedPropertyDescriptors()
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
	return SERVICE_PROPERTIES;
    }

    /*
     * @see com.sitewhere.nifi.controllers.SiteWhereContextService#getInstanceId()
     */
    @Override
    public String getInstanceId() {
	return getConfigurationContext().getProperty(INSTANCE_ID).getValue();
    }

    /*
     * @see com.sitewhere.nifi.controllers.SiteWhereContextService#getTenantId()
     */
    @Override
    public String getTenantId() {
	return getConfigurationContext().getProperty(TENANT_ID).getValue();
    }
}

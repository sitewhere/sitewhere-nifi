/*
 * Copyright (c) SiteWhere, LLC. All rights reserved. http://www.sitewhere.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.sitewhere.nifi.processors;

import org.apache.commons.text.lookup.StringLookup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sitewhere.nifi.controllers.SiteWhereContextService;

/**
 * Handles variable substitution in configuration attributes.
 */
public class SiteWhereStringLookup implements StringLookup {

    /** Static logger instance */
    @SuppressWarnings("unused")
    private static Logger LOGGER = LoggerFactory.getLogger(SiteWhereStringLookup.class);

    /** Replace with token of configured instance id */
    private static final String PRODUCT_TOKEN = "sitewhere.product";

    /** Replace with token of configured instance id */
    private static final String INSTANCE_TOKEN = "sitewhere.instance";

    /** Replace with token of configured tenant id */
    private static final String TENANT_TOKEN = "sitewhere.tenant";

    /** Context service */
    private SiteWhereContextService context;

    public SiteWhereStringLookup(SiteWhereContextService context) {
	this.context = context;
    }

    /*
     * @see org.apache.commons.text.lookup.StringLookup#lookup(java.lang.String)
     */
    @Override
    public String lookup(String key) {
	if (key.equals(PRODUCT_TOKEN)) {
	    return "sitewhere";
	} else if (key.equals(INSTANCE_TOKEN)) {
	    return getContext().getInstanceId();
	} else if (key.equals(TENANT_TOKEN)) {
	    return getContext().getTenantId();
	}
	return key;
    }

    protected SiteWhereContextService getContext() {
	return context;
    }
}

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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.text.StringSubstitutor;

import com.sitewhere.nifi.controllers.SiteWhereContextService;

public class StreamConstants {

    /** Common prefix for all SiteWhere topics */
    public static final String TOPIC_COMMON_PREFIX = "${sitewhere.product}.${sitewhere.instance}.tenant.${sitewhere.tenant}.";

    /** Prefix for streams supplied by event sources microservice */
    public static final String FA_EVENT_SOURCES_PREFIX = "event-sources-";

    /** Value for event sources decoded events stream */
    public static final String VALUE_EVENT_SOURCES_DECODED_EVENTS = FA_EVENT_SOURCES_PREFIX + "decoded-events";

    /** Value for event sources decoded events stream */
    public static final String TOPIC_EVENT_SOURCES_DECODED_EVENTS = TOPIC_COMMON_PREFIX + "event-source-decoded-events";

    public static Map<String, String> TOPICS_BY_VALUE = new HashMap<>();

    static {
	TOPICS_BY_VALUE.put(VALUE_EVENT_SOURCES_DECODED_EVENTS, TOPIC_EVENT_SOURCES_DECODED_EVENTS);
    }

    /**
     * Get topic associated with the given value.
     * 
     * @param contextService
     * @param value
     * @return
     */
    public static String getTopic(SiteWhereContextService contextService, String value) {
	String topicWithPlaceholders = TOPICS_BY_VALUE.get(value);
	if (topicWithPlaceholders == null) {
	    return null;
	}
	SiteWhereStringLookup lookup = new SiteWhereStringLookup(contextService);
	return new StringSubstitutor(lookup).replace(topicWithPlaceholders);
    }
}

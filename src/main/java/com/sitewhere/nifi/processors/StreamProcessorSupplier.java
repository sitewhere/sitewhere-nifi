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

import java.util.concurrent.ArrayBlockingQueue;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

/**
 * Handles processing of messages from SiteWhere stream.
 */
public class StreamProcessorSupplier implements ProcessorSupplier<String, byte[]> {

    /** Buffered queue */
    private ArrayBlockingQueue<BinaryPayload> queue = new ArrayBlockingQueue<>(1);

    protected ArrayBlockingQueue<BinaryPayload> getQueue() {
	return queue;
    }

    /*
     * @see org.apache.kafka.streams.processor.ProcessorSupplier#get()
     */
    @Override
    public Processor<String, byte[]> get() {
	return new Processor<String, byte[]>() {

	    @SuppressWarnings("unused")
	    private ProcessorContext context;

	    /*
	     * @see
	     * org.apache.kafka.streams.processor.Processor#init(org.apache.kafka.streams.
	     * processor.ProcessorContext)
	     */
	    @Override
	    public void init(ProcessorContext context) {
		this.context = context;
	    }

	    /*
	     * @see org.apache.kafka.streams.processor.Processor#process(java.lang.Object,
	     * java.lang.Object)
	     */
	    @Override
	    public void process(String key, byte[] event) {
		BinaryPayload payload = new BinaryPayload();
		payload.setKey(key);
		payload.setPayload(event);
		try {
		    getQueue().put(payload);
		} catch (InterruptedException e) {
		    throw new RuntimeException("Interrupted while processing record.");
		}
	    }

	    /*
	     * @see org.apache.kafka.streams.processor.Processor#close()
	     */
	    @Override
	    public void close() {
	    }
	};
    }
}

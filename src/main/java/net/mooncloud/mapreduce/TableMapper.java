/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.mooncloud.mapreduce;

import java.io.IOException;

import net.mooncloud.Record;
import net.mooncloud.io.InputSplitFile;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A {@link Mapper} which wraps a given one to allow custom
 * {@link Mapper.Context} implementations.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TableMapper<KEYOUT, VALUEOUT> extends
		Mapper<InputSplitFile, Record, KEYOUT, VALUEOUT> {

	/**
	 * Called once at the beginning of the task.
	 */
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// NOTHING
	}

	/**
	 * Called once for each key/value pair in the input split. Most applications
	 * should override this, but the default is the identity function.
	 */
	protected void map(InputSplitFile key, Record value, Context context)
			throws IOException, InterruptedException {
		// NOTHING
	}

	/**
	 * Called once at the end of the task.
	 */
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// NOTHING
	}

	/**
	 * Expert users can override this method for more complete control over the
	 * execution of the Mapper.
	 * 
	 * @param context
	 * @throws IOException
	 */
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		try {
			while (context.nextKeyValue()) {
				InputSplitFile currentKey = context.getCurrentKey();
				Record currentValue = context.getCurrentValue();
				if (currentKey == null || currentValue == null)
					continue;
				this.CounterCount(
						context,
						"Input Tables",
						1,
						"[" + currentKey.getTableName() + "]_["
								+ currentKey.getDt() + "]");
				map(currentKey, currentValue, context);
			}
		} finally {
			cleanup(context);
		}
	}

	public void CounterCount(Context context, String groupName, long incr,
			String counterName) {
		context.getCounter(groupName, counterName).increment(incr);
	}
}

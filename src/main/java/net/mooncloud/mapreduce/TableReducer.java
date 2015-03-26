package net.mooncloud.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import net.mooncloud.Record;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A {@link Reducer} which wraps a given one to allow for custom {@link Reducer.Context} implementations.
 */
public class TableReducer<KEYIN, VALUEIN> extends Reducer<KEYIN, VALUEIN, Record, NullWritable> {

	/**
	 * Called once at the start of the task.
	 */
	protected void setup(Context context) throws IOException, InterruptedException {
		// NOTHING
	}

	/**
	 * This method is called once for each key. Most applications will define their reduce class by overriding this
	 * method. The default implementation is an identity function.
	 */
	protected void reduce(KEYIN key, Iterable<VALUEIN> values, Context context) throws IOException,
			InterruptedException {
		// NOTHING
	}

	/**
	 * Called once at the end of the task.
	 */
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// NOTHING
	}

	/**
	 * Advanced application writers can use the {@link #run(org.apache.hadoop.mapreduce.Reducer.Context)} method to
	 * control how the reduce task works.
	 */
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		try {
			while (context.nextKey()) {
				reduce(context.getCurrentKey(), context.getValues(), context);
				// If a back up store is used, reset it
				Iterator<VALUEIN> iter = context.getValues().iterator();
				if (iter instanceof ReduceContext.ValueIterator) {
					((ReduceContext.ValueIterator<VALUEIN>) iter).resetBackupStore();
				}
			}
		} finally {
			cleanup(context);
		}
	}

	// /**
	// * A a wrapped {@link Reducer.Context} for custom implementations.
	// *
	// * @param reduceContext
	// * <code>ReduceContext</code> to be wrapped
	// * @return a wrapped <code>Reducer.Context</code> for custom implementations
	// */
	// public Context getReducerContext(ReduceContext<KEYIN, VALUEIN, Record, NullWritable> reduceContext) {
	// System.out.println(reduceContext.getClass().toString());
	// reduceContext.getCounter("ccc", reduceContext.getClass().toString()).increment(1);
	// return new Context(reduceContext);
	// }

	// public class Context extends Reducer<KEYIN, VALUEIN, Record, NullWritable>.Context {
	//
	// protected ReduceContext<KEYIN, VALUEIN, Record, NullWritable> reduceContext;
	//
	// public Context(ReduceContext<KEYIN, VALUEIN, Record, NullWritable> reduceContext) {
	// this.reduceContext = reduceContext;
	// }
	//
	// public Record createOutputRecord() {
	// return new Record(this.getConfiguration().get("mapred.output.schema"));
	// }
	//
	// @Override
	// public KEYIN getCurrentKey() throws IOException, InterruptedException {
	// return reduceContext.getCurrentKey();
	// }
	//
	// @Override
	// public VALUEIN getCurrentValue() throws IOException, InterruptedException {
	// return reduceContext.getCurrentValue();
	// }
	//
	// @Override
	// public boolean nextKeyValue() throws IOException, InterruptedException {
	// return reduceContext.nextKeyValue();
	// }
	//
	// @Override
	// public Counter getCounter(Enum counterName) {
	// return reduceContext.getCounter(counterName);
	// }
	//
	// @Override
	// public Counter getCounter(String groupName, String counterName) {
	// return reduceContext.getCounter(groupName, counterName);
	// }
	//
	// @Override
	// public OutputCommitter getOutputCommitter() {
	// return reduceContext.getOutputCommitter();
	// }
	//
	// @Override
	// public void write(Record key, NullWritable value) throws IOException, InterruptedException {
	// reduceContext.write(key, value);
	// }
	//
	// public void write(Record key) throws IOException, InterruptedException {
	// reduceContext.write(key, NullWritable.get());
	// }
	//
	// @Override
	// public String getStatus() {
	// return reduceContext.getStatus();
	// }
	//
	// @Override
	// public TaskAttemptID getTaskAttemptID() {
	// return reduceContext.getTaskAttemptID();
	// }
	//
	// @Override
	// public void setStatus(String msg) {
	// reduceContext.setStatus(msg);
	// }
	//
	// @Override
	// public Path[] getArchiveClassPaths() {
	// return reduceContext.getArchiveClassPaths();
	// }
	//
	// @Override
	// public String[] getArchiveTimestamps() {
	// return reduceContext.getArchiveTimestamps();
	// }
	//
	// @Override
	// public URI[] getCacheArchives() throws IOException {
	// return reduceContext.getCacheArchives();
	// }
	//
	// @Override
	// public URI[] getCacheFiles() throws IOException {
	// return reduceContext.getCacheFiles();
	// }
	//
	// @Override
	// public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
	// return reduceContext.getCombinerClass();
	// }
	//
	// @Override
	// public Configuration getConfiguration() {
	// return reduceContext.getConfiguration();
	// }
	//
	// @Override
	// public Path[] getFileClassPaths() {
	// return reduceContext.getFileClassPaths();
	// }
	//
	// @Override
	// public String[] getFileTimestamps() {
	// return reduceContext.getFileTimestamps();
	// }
	//
	// @Override
	// public RawComparator<?> getCombinerKeyGroupingComparator() {
	// return reduceContext.getCombinerKeyGroupingComparator();
	// }
	//
	// @Override
	// public RawComparator<?> getGroupingComparator() {
	// return reduceContext.getGroupingComparator();
	// }
	//
	// @Override
	// public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
	// return reduceContext.getInputFormatClass();
	// }
	//
	// @Override
	// public String getJar() {
	// return reduceContext.getJar();
	// }
	//
	// @Override
	// public JobID getJobID() {
	// return reduceContext.getJobID();
	// }
	//
	// @Override
	// public String getJobName() {
	// return reduceContext.getJobName();
	// }
	//
	// @Override
	// public boolean getJobSetupCleanupNeeded() {
	// return reduceContext.getJobSetupCleanupNeeded();
	// }
	//
	// @Override
	// public boolean getTaskCleanupNeeded() {
	// return reduceContext.getTaskCleanupNeeded();
	// }
	//
	// @Override
	// public Path[] getLocalCacheArchives() throws IOException {
	// return reduceContext.getLocalCacheArchives();
	// }
	//
	// @Override
	// public Path[] getLocalCacheFiles() throws IOException {
	// return reduceContext.getLocalCacheFiles();
	// }
	//
	// @Override
	// public Class<?> getMapOutputKeyClass() {
	// return reduceContext.getMapOutputKeyClass();
	// }
	//
	// @Override
	// public Class<?> getMapOutputValueClass() {
	// return reduceContext.getMapOutputValueClass();
	// }
	//
	// @Override
	// public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
	// return reduceContext.getMapperClass();
	// }
	//
	// @Override
	// public int getMaxMapAttempts() {
	// return reduceContext.getMaxMapAttempts();
	// }
	//
	// @Override
	// public int getMaxReduceAttempts() {
	// return reduceContext.getMaxReduceAttempts();
	// }
	//
	// @Override
	// public int getNumReduceTasks() {
	// return reduceContext.getNumReduceTasks();
	// }
	//
	// @Override
	// public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
	// return reduceContext.getOutputFormatClass();
	// }
	//
	// @Override
	// public Class<?> getOutputKeyClass() {
	// return reduceContext.getOutputKeyClass();
	// }
	//
	// @Override
	// public Class<?> getOutputValueClass() {
	// return reduceContext.getOutputValueClass();
	// }
	//
	// @Override
	// public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
	// return reduceContext.getPartitionerClass();
	// }
	//
	// @Override
	// public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
	// return reduceContext.getReducerClass();
	// }
	//
	// @Override
	// public RawComparator<?> getSortComparator() {
	// return reduceContext.getSortComparator();
	// }
	//
	// @Override
	// public boolean getSymlink() {
	// return reduceContext.getSymlink();
	// }
	//
	// @Override
	// public Path getWorkingDirectory() throws IOException {
	// return reduceContext.getWorkingDirectory();
	// }
	//
	// @Override
	// public void progress() {
	// reduceContext.progress();
	// }
	//
	// @Override
	// public Iterable<VALUEIN> getValues() throws IOException, InterruptedException {
	// return reduceContext.getValues();
	// }
	//
	// @Override
	// public boolean nextKey() throws IOException, InterruptedException {
	// return reduceContext.nextKey();
	// }
	//
	// @Override
	// public boolean getProfileEnabled() {
	// return reduceContext.getProfileEnabled();
	// }
	//
	// @Override
	// public String getProfileParams() {
	// return reduceContext.getProfileParams();
	// }
	//
	// @Override
	// public IntegerRanges getProfileTaskRange(boolean isMap) {
	// return reduceContext.getProfileTaskRange(isMap);
	// }
	//
	// @Override
	// public String getUser() {
	// return reduceContext.getUser();
	// }
	//
	// @Override
	// public Credentials getCredentials() {
	// return reduceContext.getCredentials();
	// }
	//
	// @Override
	// public float getProgress() {
	// return reduceContext.getProgress();
	// }
	// }
}

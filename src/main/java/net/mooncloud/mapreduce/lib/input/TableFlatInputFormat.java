package net.mooncloud.mapreduce.lib.input;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import net.mooncloud.Record;
import net.mooncloud.io.InputSplitFile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.StringUtils;

/**
 * 
 * 以指定的分隔符解析数据，并把数据封装到Record中，作为mapper的value。 默认分隔符为","，可以通过设置参数自定义分割符 -D
 * mapreduce.input.recordreader.value.delimiter="\001"
 * 
 * @author jdyang
 */
public class TableFlatInputFormat extends
		FileInputFormat<InputSplitFile, Record> {

	@Override
	public RecordReader<InputSplitFile, Record> createRecordReader(
			InputSplit split, TaskAttemptContext context) {
		return new FlatTableRecordReader();
	}

	/**
	 * Add a {@link Path} to the list of inputs for the map-reduce job.
	 * 
	 * @param job
	 *            The {@link Job} to modify
	 * @param path
	 *            {@link Path} to be added to the list of inputs for the
	 *            map-reduce job.
	 */
	public static void addInputPath(Job job, Path path) throws IOException {
		Configuration conf = job.getConfiguration();
		path = path.getFileSystem(conf).makeQualified(path);

		String dirStr = StringUtils.escapeString(path.toString());
		String dirs = conf.get("mapred.input.dir");
		conf.set("mapred.input.dir", dirs == null ? dirStr : dirs + ","
				+ dirStr);
	}
}

class FlatTableRecordReader extends RecordReader<InputSplitFile, Record> {
	private static final Log LOG = LogFactory.getLog(LineRecordReader.class);

	public static final String VALUE_DELIMITER = "mapreduce.input.recordreader.value.delimiter";

	private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
	private int bufferSize = DEFAULT_BUFFER_SIZE;

	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private LineReader in;
	private int maxLineLength;
	private InputSplitFile key = null;
	private Record value = null;
	private Text valueText = null;

	private Path splitPath;
	private Path ymdPath;
	private Path tablePath;
	private String schema = "";

	private String defaultValueDelimiter = ",";

	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
				Integer.MAX_VALUE);
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();
		compressionCodecs = new CompressionCodecFactory(job);
		final CompressionCodec codec = compressionCodecs.getCodec(file);

		// 我的自定i
		initializeTabelSchema(job, file);
		// 我的自定i

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		boolean skipFirstLine = false;
		if (codec != null) {
			in = new LineReader(codec.createInputStream(fileIn), job);
			end = Long.MAX_VALUE;
		} else {
			if (start != 0) {
				skipFirstLine = true;
				--start;
				fileIn.seek(start);
			}
			in = new LineReader(fileIn, job);
		}
		if (skipFirstLine) { // skip first line and re-establish "start".
			start += in.readLine(new Text(), 0,
					(int) Math.min((long) Integer.MAX_VALUE, end - start));
		}
		this.pos = start;
	}

	private void initializeTabelSchema(Configuration job, final Path file)
			throws IOException {
		FileSystem fs = file.getFileSystem(job);
		defaultValueDelimiter = job.get(VALUE_DELIMITER, defaultValueDelimiter);
		splitPath = file;
		Path ymdPathT = splitPath.getParent();
		Path tablePathT = ymdPathT.getParent();
		// table
		String[] tabNames = job.getStrings("mapred.input.table",
				new String[] {});
		for (String tn : tabNames) {
			Path tnPath = new Path(job.get("table_parent", "") + tn);
			if (ymdPathT.toString().startsWith(tnPath.toString())) {
				tablePathT = tnPath;
				break;
			}
		}
		// schema
		if (null == tablePath || !tablePath.equals(tablePathT)) {
			tablePath = tablePathT;
			Path schemaPath = new Path(tablePathT, "__schema__");
			if (!fs.isFile(schemaPath)) {
				// schema = job.get("schema"); // schema = "record:string";
				// if (schema == null)
				throw new IOException("table: " + tablePathT.getName()
						+ " not find file __schema__");
			} else {
				FSDataInputStream fsdis = fs.open(schemaPath);
				schema = fsdis.readLine();
				fsdis.close();
			}
			if (schema.endsWith(":")) {
				int firstCommaIndex = schema.indexOf(',');
				int lastCommaIndex = schema.lastIndexOf(',');
				String table = schema.substring(0, firstCommaIndex);
				table = table.replace('.', '/'); // .replace(System.getProperty("odps.project.name",
													// "mr_dw"), "")
				Path t = new Path(job.get("table_parent", "") + table);
				if (!(t.equals(tablePath)
						|| tablePath.toString().endsWith(t.toString()) || t
						.toString().endsWith(tablePath.toString()))) {
					throw new IOException("table: " + tablePathT
							+ " not equals " + t + " of __schema__");
				}
				schema = schema.substring(firstCommaIndex + 1, lastCommaIndex);
			}
		}
		if (null == ymdPath || !ymdPath.equals(ymdPathT)) {
			ymdPath = ymdPathT;
			String tableNameDt = tablePath.getName() + "_" + ymdPath.getName();
			String[] strings = job.getStrings("map_input", null);
			HashSet<String> tabelNames = new HashSet<String>(1);
			tabelNames.add(tableNameDt);
			if (strings != null) {
				tabelNames.addAll(Arrays.asList(strings));
			}
			job.setStrings("map_input", tabelNames.toArray(new String[0]));
		}
	}

	private int split_limit = -1;
	public boolean nextKeyValue() throws IOException {
		if (key == null
				|| !(splitPath.getName().equals(key.getFileName()) && tablePath
						.getName().equals(key.getTableName()))) {
			key = new InputSplitFile();
			key.setFileName(splitPath.getName());
			key.setDt(ymdPath.toString()
					.substring(tablePath.toString().length())
					.replaceFirst("/", ""));
			key.setTableName(tablePath.getName());
			key.setFilePath(splitPath);
		}
		if (valueText == null) {
			valueText = new Text();
		}
		if (value == null || !(tablePath.getName().equals(key.getTableName()))) {
			value = new Record(schema);
			split_limit = value.getFields().length;
		}
		int newSize = 0;
		while (pos < end) {
			newSize = in.readLine(valueText, maxLineLength,
					Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
							maxLineLength));
			if (newSize == 0) {
				break;
			}
			String[] values = valueText.toString().split(defaultValueDelimiter, split_limit); // ,
																					// value.getFields().length);
			try {
				value.setAll(values);
			} catch (Exception e) {
				value = null;
				// if(!"taobao_buyer_item".equals(key.getTableName()))
				// {
				// System.out.println(key.getFilePath());
				// System.out.println(defaultValueDelimiter);
				 for (String v : values)
				 {
				 System.out.print(v + "#@#");
				 }
				 System.out.println();
				 throw new IOException(e);
				// }
			}
			pos += newSize;
			if (newSize < maxLineLength) {
				break;
			}

			// line too long. try again
			LOG.info("Skipped line of size " + newSize + " at pos "
					+ (pos - newSize));
		}
		if (newSize == 0) {
			key = null;
			value = null;
			valueText = null;
			return false;
		} else {
			return true;
		}

	}

	@Override
	public InputSplitFile getCurrentKey() {
		return key;
	}

	@Override
	public Record getCurrentValue() {
		return value;
	}

	/**
	 * Get the progress within the split
	 */
	public float getProgress() {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	public synchronized void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}
}

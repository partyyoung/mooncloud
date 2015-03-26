package net.mooncloud.mapreduce.lib.output;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;

import net.mooncloud.Record;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import com.csvreader.CsvWriter;

/**
 * 
 * 调用{@link CsvWriter}以指定的Csv分隔符链接Record的字段 默认分隔符为','，可以通过设置参数自定义分割符 -D
 * mapred.csvoutputformat.value.delimiter=1
 * 
 * @author Administrator
 * 
 */
public class CsvOutputFormat extends FileOutputFormat<Record, NullWritable> {
	private static final Log LOG = LogFactory.getLog(CsvOutputFormat.class);

	public static final String VALUE_DELIMITER = "mapred.csvoutputformat.value.delimiter";
	private static final char defaultCsvDelimiter = ',';

	protected static class CsvRecordWriter extends
			RecordWriter<Record, NullWritable> {
		private static final String utf8 = "UTF-8";
		private static final byte[] newline;
		static {
			try {
				newline = "\n".getBytes(utf8);
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + utf8
						+ " encoding");
			}
		}

		protected DataOutputStream out;
		private final byte[] keyValueSeparator;
		private final char valueDelimiter;

		public CsvRecordWriter(DataOutputStream out, String keyValueSeparator,
				char valueDelimiter) {
			this.out = out;
			try {
				this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
				this.valueDelimiter = valueDelimiter;
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + utf8
						+ " encoding");
			}
		}

		public CsvRecordWriter(DataOutputStream out, String keyValueSeparator) {
			this(out, keyValueSeparator, defaultCsvDelimiter);
		}

		public CsvRecordWriter(DataOutputStream out) {
			this(out, "\t");
		}

		/**
		 * Write the object to the byte stream, handling Text as a special case.
		 * 
		 * @param o
		 *            the object to print
		 * @throws IOException
		 *             if the write throws, we pass it on
		 */
		private void writeObject(Object o) throws IOException {
			if (o instanceof Text) {
				Text to = (Text) o;
				out.write(to.getBytes(), 0, to.getLength());
			} else if (o instanceof Record) {
				StringWriter writer = new StringWriter();
				CsvWriter csvWriter = new CsvWriter(writer, valueDelimiter);
				Writable[] values = ((Record) o).getAll();
				String[] stringValues = new String[values.length];
				for (int i = 0; i < values.length; i++) {
					try {
						if (values[i] == null) {
							stringValues[i] = "";
							continue;
						}
						stringValues[i] = values[i].toString();
					} catch (Exception e) {
						stringValues[i] = "";
						System.out.println(((Record) o).getField(i));
					}
				}
				csvWriter.writeRecord(stringValues);
				out.write(writer.toString().getBytes(utf8));
				csvWriter.close();
				writer.close();
			} else {
				out.write(o.toString().getBytes(utf8));
			}
		}

		public synchronized void write(Record key, NullWritable value)
				throws IOException {

			boolean nullKey = key == null; // || key instanceof NullWritable;
			boolean nullValue = value == null || value instanceof NullWritable;
			if (nullKey && nullValue) {
				return;
			}
			if (!nullKey) {
				writeObject(key);
			}
			if (!(nullKey || nullValue)) {
				out.write(keyValueSeparator);
			}
			if (!nullValue) {
				writeObject(value);
			}
			// out.write(newline);
		}

		public synchronized void close(TaskAttemptContext context)
				throws IOException {
			out.close();
		}
	}

	@Override
	public RecordWriter<Record, NullWritable> getRecordWriter(
			TaskAttemptContext job) throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		boolean isCompressed = getCompressOutput(job);
		String keyValueSeparator = conf.get(
				"mapred.textoutputformat.separator", "\t");
		char valueDelimiter = (char) conf.getInt(VALUE_DELIMITER,
				defaultCsvDelimiter);
		CompressionCodec codec = null;
		String extension = "";
		if (isCompressed) {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
					job, GzipCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,
					conf);
			extension = codec.getDefaultExtension();
		}
		Path file = getDefaultWorkFile(job, extension);
		FileSystem fs = file.getFileSystem(conf);
		if (!isCompressed) {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new CsvRecordWriter(fileOut, keyValueSeparator,
					valueDelimiter);
		} else {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new CsvRecordWriter(new DataOutputStream(
					codec.createOutputStream(fileOut)), keyValueSeparator,
					valueDelimiter);
		}
	}

	/**
	 * Get the {@link Path} to the output directory for the map-reduce job.
	 * 
	 * @return the {@link Path} to the output directory for the map-reduce job.
	 * @see FileOutputFormat#getWorkOutputPath(TaskInputOutputContext)
	 */
	@SuppressWarnings("deprecation")
	public static void setOutputPath(Job job, Path outputDir) {
		Path tablePath = outputDir.getParent();
		Path schemaPath = new Path(outputDir.getParent(), "__schema__");
		try {
			FileSystem fs = schemaPath.getFileSystem(job.getConfiguration());
			if (!fs.isFile(schemaPath)) {
				throw new IOException("table: " + tablePath.getName()
						+ " not find file __schema__");
			}
			FSDataInputStream fsdis = fs.open(schemaPath);
			String schema = fsdis.readLine();
			fsdis.close();
			if (schema.endsWith(":")) {
				int firstCommaIndex = schema.indexOf(',');
				int lastCommaIndex = schema.lastIndexOf(',');
				String table = schema.substring(0, firstCommaIndex);
				table = table.replace('.', '/'); // .replace("mr_dw", "")
				Path t = new Path(job.getConfiguration()
						.get("table_parent", "") + table);
				if (!t.equals(tablePath)) {
					throw new IOException("table: " + tablePath
							+ " not equals " + t + " of __schema__");
				}
				schema = schema.substring(firstCommaIndex + 1, lastCommaIndex);
			}
			LOG.info("mapred.output.schema = " + schema);
			job.getConfiguration().set("mapred.output.schema", schema);
		} catch (IOException e) {
			e.printStackTrace();
		}

		job.setOutputFormatClass(CsvOutputFormat.class);

		job.getConfiguration().set("mapred.output.dir", outputDir.toString());
	}

	public static void main(String[] args) {
		DBOutputFormat<Record, NullWritable> dbOutputFormat = new DBOutputFormat<Record, NullWritable>();
		String[] fieldNames = { "thedate", "shop_id" };
		System.out.println(dbOutputFormat.constructQuery("table_name",
				fieldNames));
	}
}

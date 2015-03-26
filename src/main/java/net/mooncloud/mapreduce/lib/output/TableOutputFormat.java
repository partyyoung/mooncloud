package net.mooncloud.mapreduce.lib.output;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import net.mooncloud.Record;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

public class TableOutputFormat extends FileOutputFormat<Record, NullWritable> {
	private static final Log LOG = LogFactory.getLog(TableOutputFormat.class);

	public static final String VALUE_DELIMITER = "mapreduce.output.recordwriter.value.delimiter";
	private static String defaultValueDelimiter = ",";

	protected static class TableRecordWriter extends
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
		private final String valueDelimiter;

		public TableRecordWriter(DataOutputStream out,
				String keyValueSeparator, String valueDelimiter) {
			this.out = out;
			try {
				this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
				this.valueDelimiter = valueDelimiter;
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + utf8
						+ " encoding");
			}
		}

		public TableRecordWriter(DataOutputStream out) {
			this(out, "\t", defaultValueDelimiter);
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
				Record r = (Record) o;
				out.write(r.toString(this.valueDelimiter).getBytes(utf8));
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
			out.write(newline);
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
		String valueDelimiter = conf
				.get(VALUE_DELIMITER, defaultValueDelimiter);
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
			return new TableRecordWriter(fileOut, keyValueSeparator,
					valueDelimiter);
		} else {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new TableRecordWriter(new DataOutputStream(
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
				table = table.replace('.', '/');// .replace(System.getProperty("odps.project.name",
												// "mr_dw"), "")
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

		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set("mapred.output.dir", outputDir.toString());
	}

	@SuppressWarnings("deprecation")
	public static void loadDataToDB(Job job) throws ClassNotFoundException,
			SQLException, IOException, InterruptedException {
		// 创连接
		DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());
		String db_table = dbConf.getOutputTableName();
		String[] fieldNames = dbConf.getOutputFieldNames();
		if (fieldNames == null) {
			fieldNames = new String[dbConf.getOutputFieldCount()];
		}
		Connection connection = null;
		PreparedStatement statement = null;
		try {
			LOG.info("Connect to "
					+ job.getConfiguration().get(DBConfiguration.URL_PROPERTY));
			connection = dbConf.getConnection();
			connection.setAutoCommit(false);

			// 先删除
			String db_table_dt = job.getConfiguration().get("db_table_dt");
			String delete = "DELETE FROM " + db_table_dt;
			Statement stmt = connection.createStatement();
			int dc = stmt.executeUpdate(delete);
			stmt.close();
			LOG.info("DELETE FROM " + db_table_dt + " : " + dc);

		} catch (Exception e) {
			e.printStackTrace();
			try {
				connection.rollback();
			} catch (SQLException ex) {
				ex.printStackTrace();
				LOG.warn(StringUtils.stringifyException(ex));
			}
			throw new IOException(e.getMessage());
		}

		// 再插入
		DBOutputFormat<Record, NullWritable> dbOutputFormat = new DBOutputFormat<Record, NullWritable>();
		statement = connection.prepareStatement(dbOutputFormat.constructQuery(
				db_table, fieldNames));
		RecordWriter<Record, NullWritable> recordWriter = dbOutputFormat.new DBRecordWriter(
				connection, statement);
		Path outputDir = TableOutputFormat.getOutputPath(job);
		String table_schema = job.getConfiguration()
				.get("mapred.output.schema");
		LOG.info("loading data from table:'" + outputDir.toString()
				+ "' to db:'" + db_table + "'");

		int dc = 0;
		try {
			FileSystem fs = outputDir.getFileSystem(job.getConfiguration());
			if (!fs.getFileStatus(outputDir).isDir()) {
				throw new IOException("table: " + outputDir.getName()
						+ " not exist");
			}
			FileStatus[] fileStatus = fs.listStatus(outputDir,
					new PathFilter() {
						@Override
						public boolean accept(Path path) {
							if (path.toString().startsWith("_")) {
								return false;
							}
							return true;
						}
					});
			FSDataInputStream fsdis = null;
			Record record = new Record(table_schema);
			dc = 0;
			for (FileStatus fileSta : fileStatus) {
				if (fileSta.isDir()) {
					continue;
				}
				fsdis = fs.open(fileSta.getPath());
				String line;
				while ((line = fsdis.readLine()) != null) {
					record.setAll(line.split(
							job.getConfiguration().get(VALUE_DELIMITER,
									defaultValueDelimiter), -1));
					recordWriter.write(record, NullWritable.get());
					dc++;
				}
			}
			fsdis.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		recordWriter.close(null);
		LOG.info("INSERT INTO " + db_table + " : " + dc);
	}

	public static void main(String[] args) {
		DBOutputFormat<Record, NullWritable> dbOutputFormat = new DBOutputFormat<Record, NullWritable>();
		String[] fieldNames = { "thedate", "shop_id" };
		System.out.println(dbOutputFormat.constructQuery("table_name",
				fieldNames));
	}
}

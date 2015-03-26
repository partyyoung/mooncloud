package net.mooncloud.mapreduce.lib.input;

import java.io.IOException;
import java.nio.charset.Charset;
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
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.StringUtils;

import com.csvreader.CsvReader;

/**
 * 
 * 调用{@link CsvReader}以指定的Csv分隔符解析数据，并把数据封装到Record中，作为mapper的value。
 * 默认分隔符为','，可以通过设置参数自定义分割符 -D mapred.csvinputformat.value.delimiter=1
 * 注意：自定义的分隔符只支持char字符，请指定字符分隔符对应的ASCII码。 如果指定了参数为字符串，字符串的第一个字符将作为分隔符
 * 
 * @author yangjiangdang
 */
public class CsvInputFormat extends CombineFileInputFormat<InputSplitFile, Record>
{
	public static final String CREATE_DIR = "mapreduce.jobcontrol.createdir.ifnotexist";

	@Override
	public RecordReader<InputSplitFile, Record> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException
	{
		return new CombineFileRecordReader<InputSplitFile, Record>((CombineFileSplit) split, context, CsvRecordReader.class);
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
	public static void addInputPath(Job job, Path path) throws IOException
	{
		Configuration conf = job.getConfiguration();
		path = path.getFileSystem(conf).makeQualified(path);

//		if (conf.getBoolean(CREATE_DIR, true))
//		{
//			System.out.println("path=" + path);
//			FileSystem fs = FileSystem.get(conf);
//			if (!fs.exists(path))
//			{
//				try
//				{
//					fs.mkdirs(path);
//				}
//				catch (IOException e)
//				{
//					e.printStackTrace();
//				}
//			}
//		}

		String dirStr = StringUtils.escapeString(path.toString());
		String dirs = conf.get("mapred.input.dir");
		conf.set("mapred.input.dir", dirs == null ? dirStr : dirs + "," + dirStr);
	}
}

class CsvRecordReader extends RecordReader<InputSplitFile, Record>
{
	private static final Log LOG = LogFactory.getLog(LineRecordReader.class);

	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private CsvReader in;
	private int maxLineLength;
	private InputSplitFile key = null;
	private Record value = null;

	private Path splitPath;
	private Path ymdPath;
	private Path tablePath;
	private String schema = "";

	private char defaultValueDelimiter = ',';

	@SuppressWarnings("deprecation")
	public CsvRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException
	{
		Configuration job = context.getConfiguration();
		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
		this.start = split.getOffset(index);
		this.end = start + split.getLength(index);
		final Path file = split.getPath(index);
		compressionCodecs = new CompressionCodecFactory(job);
		final CompressionCodec codec = compressionCodecs.getCodec(file);

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);

		// 我的自定i
		defaultValueDelimiter = (char) job.getInt("mapred.csvinputformat.value.delimiter", defaultValueDelimiter);
		splitPath = file;
		Path ymdPathT = splitPath.getParent();
		Path tablePathT = ymdPathT.getParent();
		if (null == tablePath || !tablePath.equals(tablePathT))
		{
			Path schemaPath = new Path(tablePathT, "_schema_");
			if (!fs.isFile(schemaPath))
			{
				tablePathT = ymdPathT;
				schemaPath = new Path(tablePathT, "_schema_");
				if (!fs.isFile(schemaPath))
				{
					throw new IOException("table: " + tablePathT.getName() + " not find file _schema_");
				}
			}
			tablePath = tablePathT;
			FSDataInputStream fsdis = fs.open(schemaPath);
			schema = fsdis.readLine();
			fsdis.close();
			if (schema.endsWith(":"))
			{
				int firstCommaIndex = schema.indexOf(',');
				int lastCommaIndex = schema.lastIndexOf(',');
				String table = schema.substring(0, firstCommaIndex);
				table = table.replace(System.getProperty("odps.project.name", "mr_dw"), "").replace('.', '/');
				// if (table.contains("."))
				// {
				// table = table.substring(table.lastIndexOf('.'));
				// }
				Path t = new Path(job.get("table_parent", "") + table);
				if (!t.equals(tablePath))
				{
					throw new IOException("table: " + tablePathT + " not equals " + t + " of _schema_");
				}
				schema = schema.substring(firstCommaIndex + 1, lastCommaIndex);
			}
		}
		if (null == ymdPath || !ymdPath.equals(ymdPathT))
		{
			ymdPath = ymdPathT;
			String tableNameDt = tablePath.getName() + "_" + ymdPath.getName();
			String[] strings = job.getStrings("map_input", null);
			HashSet<String> tabelNames = new HashSet<String>(1);
			tabelNames.add(tableNameDt);
			if (strings != null)
			{
				tabelNames.addAll(Arrays.asList(strings));
			}
			job.setStrings("map_input", tabelNames.toArray(new String[0]));
		} // 我的自定i

		FSDataInputStream fileIn = fs.open(file);
		boolean skipFirstLine = false;
		if (codec != null)
		{
			in = new CsvReader(codec.createInputStream(fileIn), defaultValueDelimiter, Charset.forName("UTF-8")); // ISO-8859-1
																													// UTF-8
			end = Long.MAX_VALUE;
		}
		else
		{
			if (start != 0)
			{
				skipFirstLine = true;
				--start;
				fileIn.seek(start);
			}
			in = new CsvReader(fileIn, defaultValueDelimiter, Charset.forName("UTF-8"));
		}
		// if (skipFirstLine)
		// { // skip first line and re-establish "start".
		// start += in.readLine(new Text(), 0,
		// (int) Math.min((long) Integer.MAX_VALUE, end - start));
		// }
		this.pos = start;
	}

	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException
	{

	}

	public boolean nextKeyValue() throws IOException
	{
		if (key == null || !(splitPath.getName().equals(key.getFileName()) && tablePath.getName().equals(key.getTableName())))
		{
			key = new InputSplitFile();
			key.setFileName(splitPath.getName());
			key.setDt(ymdPath.getName());
			key.setTableName(tablePath.getName());
			key.setFilePath(splitPath);
		}
		if (value == null || !(tablePath.getName().equals(key.getTableName())))
		{
			value = new Record(schema);
		}
		int newSize = 0;
		if (in.readRecord())
		{
			String[] values = in.getValues();
			try
			{
				newSize = value.setAll(values);
				pos += newSize;
			}
			catch (Exception e)
			{
				value = null;
//				if(!"taobao_buyer_item".equals(key.getTableName()))
//				{
//				System.out.println(key.getFilePath());
//				System.out.println(defaultValueDelimiter);
//				for (String v : values)
//				{
//					System.out.print(v + "#@#");
//				}
//				System.out.println();
//				 throw new IOException(e);
//				}
			}
			return true;
		}
		else
		{
			key = null;
			value = null;
			return false;
		}
	}

	@Override
	public InputSplitFile getCurrentKey()
	{
		return key;
	}

	@Override
	public Record getCurrentValue()
	{
		return value;
	}

	/**
	 * Get the progress within the split
	 */
	public float getProgress()
	{
		if (start == end)
		{
			return 0.0f;
		}
		else
		{
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	public synchronized void close() throws IOException
	{
		if (in != null)
		{
			in.close();
		}
	}
}

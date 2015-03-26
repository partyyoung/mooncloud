package net.mooncloud.mapreduce.lib.input;

import java.io.IOException;

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
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

public class CombineFileAsTextInputFormat extends CombineFileInputFormat<InputSplitFile, Text> {
	@Override
	public RecordReader<InputSplitFile, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		return new CombineFileRecordReader<InputSplitFile, Text>((CombineFileSplit) split, context,
				MyCombineFileAsTextRecordReader.class);
	}

}

class MyCombineFileAsTextRecordReader extends RecordReader<InputSplitFile, Text> {
	private static final Log LOG = LogFactory.getLog(MyCombineFileAsTextRecordReader.class);

	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private Path path;
	private LineReader in;
	private int maxLineLength;
	private InputSplitFile key = null;
	private Text value = null;

	public MyCombineFileAsTextRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index)
			throws IOException {
		Configuration job = context.getConfiguration();
		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
		this.path = split.getPath(index);
		this.start = split.getOffset(index);
		this.end = start + split.getLength(index);
		compressionCodecs = new CompressionCodecFactory(job);
		final CompressionCodec codec = compressionCodecs.getCodec(this.path);
		boolean skipFirstLine = false;

		FileSystem fs = path.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath(index));
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
		if (skipFirstLine) // skip first line and re-establish "startOffset".
		{
			start += in.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
		}
		this.pos = start;
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException,
			InterruptedException {
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (key == null) {
			key = new InputSplitFile();
			key.setFileName(path.getName());
			key.setTableName(path.getParent().getParent().getName());
		}
		key.setOffset(pos);
		if (value == null) {
			value = new Text();
		}
		int newSize = 0;
		while (pos < end) {
			try {
				newSize = in.readLine(value, maxLineLength,
						Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
			} catch (Exception e) {
				e.printStackTrace();
				newSize = 0;
			}
			if (newSize == 0) {
				break;
			}
			pos += newSize;
			if (newSize < maxLineLength) {
				break;
			}

			// line too long. try again
			LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
		}
		if (newSize == 0) {
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}
	}

	@Override
	public InputSplitFile getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	@Override
	public void close() throws IOException {
		if (in != null)
			in.close();
	}

}

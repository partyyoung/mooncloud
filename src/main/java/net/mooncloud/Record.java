package net.mooncloud;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class Record implements Writable, DBWritable {
	private FieldSchema[] fields;

	private Writable[] values;

	private HashMap<String, Integer> fieldIndex;

	private int size;

	public Record() {
	}

	public Record(String schema) {
		schema(schema);
	}

	public Record(String schema, String[] value) throws IOException {
		setAll(schema, value);
	}

	private void schema(String schema) {
		String[] s = schema.split(",", -1);
		fields = new FieldSchema[s.length];
		values = new Writable[s.length];
		fieldIndex = new HashMap<String, Integer>(s.length);
		for (int i = 0; i < s.length; i++) {
			String[] nt = s[i].split(":");
			String name = nt[0];
			String type = nt[1];

			fields[i] = new FieldSchema(name, type);
			fieldIndex.put(name, i);
		}
	}

	public int setAll(String schema, String[] value) throws IOException {
		schema(schema);
		int size = setAll(value);
		return size;
	}

	public int setAll(String[] value) throws IOException {
		if (fields.length != value.length) {
			throw (IOException) new IOException("expect " + fields.length + ", but the length is " + value.length);
		}
		if (values == null) {
			values = new Writable[fields.length];
		}
		int size = 0;
		for (int i = 0; i < fields.length; i++) {
			if (value[i].startsWith("\"") && value[i].endsWith("\"")) {
				value[i] = value[i].substring(1, value[i].length() - 1);
			}
			if ("string".equals(fields[i].type)) {
				values[i] = new Text(value[i]);
			} else if ("long".equals(fields[i].type) || "bigint".equals(fields[i].type)) {
				values[i] = new LongWritable(Long.parseLong(value[i]));
			} else if ("double".equals(fields[i].type)) {
				values[i] = new DoubleWritable(Double.parseDouble(value[i]));
			}
			size += value[i].getBytes().length;
		}
		return size;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, values.length);
		for (int i = 0; i < values.length; ++i) {
			Text.writeString(out, values[i].getClass().getName());
		}
		for (int i = 0; i < values.length; ++i) {
			values[i].write(out);
		}
		for (int i = 0; i < values.length; ++i) {
			Text.writeString(out, fields[i].name);
			Text.writeString(out, fields[i].type);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		size = WritableUtils.readVInt(in);
		values = new Writable[size];
		Class<? extends Writable>[] cls = new Class[size];
		try {
			for (int i = 0; i < size; ++i) {
				cls[i] = Class.forName(Text.readString(in)).asSubclass(Writable.class);
			}
			for (int i = 0; i < size; ++i) {
				values[i] = cls[i].newInstance();
				values[i].readFields(in);
			}
			fields = new FieldSchema[size];
			fieldIndex = new HashMap<String, Integer>(size);
			for (int i = 0; i < size; ++i) {
				fields[i] = new FieldSchema(Text.readString(in), Text.readString(in));
				fieldIndex.put(fields[i].name, i);
			}
		} catch (ClassNotFoundException e) {
			throw (IOException) new IOException("Failed tuple init").initCause(e);
		} catch (IllegalAccessException e) {
			throw (IOException) new IOException("Failed tuple init").initCause(e);
		} catch (InstantiationException e) {
			throw (IOException) new IOException("Failed tuple init").initCause(e);
		}
	}

	public String toString() {
		StringBuilder sb = new StringBuilder(fields.length);
		sb.append(get(0));
		for (int i = 1; i < fields.length; i++) {
			sb.append("\001").append(get(i));
		}
		return sb.toString();
	}

	public String toString(String delimiter) {
		StringBuilder sb = new StringBuilder(fields.length);
		sb.append(get(0));
		for (int i = 1; i < fields.length; i++) {
			sb.append(delimiter).append(get(i));
		}
		return sb.toString();
	}

	public void fromDelimitedString(String arg0) throws IOException {

	}

	public void fromDelimitedString(String arg0, char arg1, String arg2) throws IOException {

	}

	public Writable[] getAll() {
		return values;
	}

	public FieldSchema getField(int arg0) {
		return fields[arg0];
	}

	public FieldSchema[] getFields() {
		return fields;
	}

	public boolean isNull(int arg0) {
		return values == null || values[arg0] == null;
	}

	public boolean isNull(String field) throws IOException {
		return values == null || get(fieldIndex.get(field)) == null;
	}

	public Writable get(String field) {
		return get(fieldIndex.get(field));
	}

	public Writable get(int i) {
		return values[i];
	}

	public void set(String field, Writable value) throws IOException {
		values[fieldIndex.get(field)] = value;
	}

	public void set(int i, Writable value) {
		values[i] = value;
	}

	public void set(Writable[] arg0) throws IOException {
		values = arg0;
	}

	public int size() {
		return size;
	}

	public String toDelimitedString() {
		return null;
	}

	public String toDelimitedString(char arg0, String arg1) {
		return null;
	}

	@Override
	public void write(PreparedStatement statement) throws SQLException {
		for (int i = 0; i < fields.length; i++) {
			if ("string".equals(fields[i].type)) {
				statement.setString(i + 1, ((Text) values[i]).toString());
			} else if ("long".equals(fields[i].type)) {
				statement.setLong(i + 1, ((LongWritable) values[i]).get());
			} else if ("double".equals(fields[i].type)) {
				statement.setDouble(i + 1, ((DoubleWritable) values[i]).get());
			}
		}
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		for (int i = 0; i < fields.length; i++) {
			if ("string".equals(fields[i].type)) {
				values[i] = new Text(resultSet.getString(i + 1));
			} else if ("long".equals(fields[i].type)) {
				values[i] = new LongWritable(Long.parseLong(resultSet.getString(i + 1)));
			} else if ("double".equals(fields[i].type)) {
				values[i] = new DoubleWritable(Double.parseDouble(resultSet.getString(i + 1)));
			}
		}

	}
}

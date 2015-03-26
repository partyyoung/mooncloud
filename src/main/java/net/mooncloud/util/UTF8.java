package net.mooncloud.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import net.mooncloud.io.DataInputBuffer;
import net.mooncloud.io.DataOutputBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class UTF8 implements Comparable<UTF8> {
	private static final Log LOG = LogFactory.getLog(UTF8.class);
	private static final DataInputBuffer IBUF = new DataInputBuffer();

	private static final ThreadLocal<DataOutputBuffer> OBUF_FACTORY = new ThreadLocal<DataOutputBuffer>() {
		@Override
		protected DataOutputBuffer initialValue() {
			return new DataOutputBuffer();
		}
	};

	private static final byte[] EMPTY_BYTES = new byte[0];

	private byte[] bytes = EMPTY_BYTES;
	private int length;

	public UTF8() {
		// set("");
	}

	/** Construct from a given string. */
	public UTF8(String string) {
		set(string);
	}

	/** Construct from a given string. */
	public UTF8(UTF8 utf8) {
		set(utf8);
	}

	/** The raw bytes. */
	public byte[] getBytes() {
		return bytes;
	}

	/** The number of bytes in the encoded string. */
	public int getLength() {
		return length;
	}

	/** Set to contain the contents of a string. */
	public void set(String string) {
		if (string.length() > 0xffff / 3) { // maybe too long
			LOG.warn("truncating long string: " + string.length()
					+ " chars, starting with " + string.substring(0, 20));
			string = string.substring(0, 0xffff / 3);
		}

		length = utf8Length(string); // compute length
		if (length > 0xffff) // double-check length
			throw new RuntimeException("string too long!");

		if (bytes == null || length > bytes.length) // grow buffer
			bytes = new byte[length];

		try { // avoid sync'd allocations
			DataOutputBuffer obuf = OBUF_FACTORY.get();
			obuf.reset();
			writeChars(obuf, string, 0, string.length());
			System.arraycopy(obuf.getData(), 0, bytes, 0, length);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/** Set to contain the contents of a string. */
	public void set(UTF8 other) {
		length = other.length;
		if (bytes == null || length > bytes.length) // grow buffer
			bytes = new byte[length];
		System.arraycopy(other.bytes, 0, bytes, 0, length);
	}

	/** Compare two UTF8s. */
	@Override
	public int compareTo(UTF8 o) {
		return compareBytes(bytes, 0, length, o.bytes, 0, o.length);
	}

	/** Convert to a String. */
	@Override
	public String toString() {
		StringBuilder buffer = new StringBuilder(length);
		try {
			synchronized (IBUF) {
				IBUF.reset(bytes, length);
				readChars(IBUF, buffer, length);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return buffer.toString();
	}

	/**
	 * Convert to a string, checking for valid UTF8.
	 * 
	 * @return the converted string
	 * @throws UTFDataFormatException
	 *             if the underlying bytes contain invalid UTF8 data.
	 */
	public String toStringChecked() throws IOException {
		StringBuilder buffer = new StringBuilder(length);
		synchronized (IBUF) {
			IBUF.reset(bytes, length);
			readChars(IBUF, buffer, length);
		}
		return buffer.toString();
	}

	/** Returns true iff <code>o</code> is a UTF8 with the same contents. */
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof UTF8))
			return false;
		UTF8 that = (UTF8) o;
		if (this.length != that.length)
			return false;
		else
			return compareBytes(bytes, 0, length, that.bytes, 0, that.length) == 0;
	}

	private int compareBytes(byte[] buffer1, int offset1, int length1,
			byte[] buffer2, int offset2, int length2) {
		// Short circuit equal case
		if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
			return 0;
		}
		// Bring WritableComparator code local
		int end1 = offset1 + length1;
		int end2 = offset2 + length2;
		for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
			int a = (buffer1[i] & 0xff);
			int b = (buffer2[j] & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return length1 - length2;
	}

	@Override
	public int hashCode() {
		int offset = 0;
		int hash = 1;
		for (int i = offset; i < offset + length; i++)
			hash = (31 * hash) + (int) bytes[i];
		return hash;
	}

	// / STATIC UTILITIES FROM HERE DOWN

	// / These are probably not used much anymore, and might be removed...

	/**
	 * Convert a string to a UTF-8 encoded byte array.
	 * 
	 * @see String#getBytes(String)
	 */
	public static byte[] getBytes(String string) {
		byte[] result = new byte[utf8Length(string)];
		try { // avoid sync'd allocations
			DataOutputBuffer obuf = OBUF_FACTORY.get();
			obuf.reset();
			writeChars(obuf, string, 0, string.length());
			System.arraycopy(obuf.getData(), 0, result, 0, obuf.getLength());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return result;
	}

	/**
	 * Convert a UTF-8 encoded byte array back into a string.
	 * 
	 * @throws IOException
	 *             if the byte array is invalid UTF8
	 */
	public static String fromBytes(byte[] bytes) throws IOException {
		DataInputBuffer dbuf = new DataInputBuffer();
		dbuf.reset(bytes, 0, bytes.length);
		StringBuilder buf = new StringBuilder(bytes.length);
		readChars(dbuf, buf, bytes.length);
		return buf.toString();
	}

	/**
	 * Read a UTF-8 encoded string.
	 * 
	 * @see DataInput#readUTF()
	 */
	public static String readString(DataInput in) throws IOException {
		int bytes = in.readUnsignedShort();
		StringBuilder buffer = new StringBuilder(bytes);
		readChars(in, buffer, bytes);
		return buffer.toString();
	}

	private static void readChars(DataInput in, StringBuilder buffer, int nBytes)
			throws UTFDataFormatException, IOException {
		DataOutputBuffer obuf = OBUF_FACTORY.get();
		obuf.reset();
		obuf.write(in, nBytes);
		byte[] bytes = obuf.getData();
		int i = 0;
		while (i < nBytes) {
			byte b = bytes[i++];
			if ((b & 0x80) == 0) {
				// 0b0xxxxxxx: 1-byte sequence
				buffer.append((char) (b & 0x7F));
			} else if ((b & 0xE0) == 0xC0) {
				if (i >= nBytes) {
					throw new UTFDataFormatException("Truncated UTF8 at "
							+ StringUtils.byteToHexString(bytes, i - 1, 1));
				}
				// 0b110xxxxx: 2-byte sequence
				buffer.append((char) (((b & 0x1F) << 6) | (bytes[i++] & 0x3F)));
			} else if ((b & 0xF0) == 0xE0) {
				// 0b1110xxxx: 3-byte sequence
				if (i + 1 >= nBytes) {
					throw new UTFDataFormatException("Truncated UTF8 at "
							+ StringUtils.byteToHexString(bytes, i - 1, 2));
				}
				buffer.append((char) (((b & 0x0F) << 12)
						| ((bytes[i++] & 0x3F) << 6) | (bytes[i++] & 0x3F)));
			} else if ((b & 0xF8) == 0xF0) {
				if (i + 2 >= nBytes) {
					throw new UTFDataFormatException("Truncated UTF8 at "
							+ StringUtils.byteToHexString(bytes, i - 1, 3));
				}
				// 0b11110xxx: 4-byte sequence
				int codepoint = ((b & 0x07) << 18)
						| ((bytes[i++] & 0x3F) << 12)
						| ((bytes[i++] & 0x3F) << 6) | ((bytes[i++] & 0x3F));
				buffer.append(highSurrogate(codepoint)).append(
						lowSurrogate(codepoint));
			} else {
				// The UTF8 standard describes 5-byte and 6-byte sequences, but
				// these are no longer allowed as of 2003 (see RFC 3629)

				// Only show the next 6 bytes max in the error code - in case
				// the
				// buffer is large, this will prevent an exceedingly large
				// message.
				int endForError = Math.min(i + 5, nBytes);
				throw new UTFDataFormatException(
						"Invalid UTF8 at "
								+ StringUtils.byteToHexString(bytes, i - 1,
										endForError));
			}
		}
	}

	private static char highSurrogate(int codePoint) {
		return (char) ((codePoint >>> 10) + (Character.MIN_HIGH_SURROGATE - (Character.MIN_SUPPLEMENTARY_CODE_POINT >>> 10)));
	}

	private static char lowSurrogate(int codePoint) {
		return (char) ((codePoint & 0x3ff) + Character.MIN_LOW_SURROGATE);
	}

	/**
	 * Write a UTF-8 encoded string.
	 * 
	 * @see DataOutput#writeUTF(String)
	 */
	public static int writeString(DataOutput out, String s) throws IOException {
		if (s.length() > 0xffff / 3) { // maybe too long
			LOG.warn("truncating long string: " + s.length()
					+ " chars, starting with " + s.substring(0, 20));
			s = s.substring(0, 0xffff / 3);
		}

		int len = utf8Length(s);
		if (len > 0xffff) // double-check length
			throw new IOException("string too long!");

		out.writeShort(len);
		writeChars(out, s, 0, s.length());
		return len;
	}

	/** Returns the number of bytes required to write this. */
	private static int utf8Length(String string) {
		int stringLength = string.length();
		int utf8Length = 0;
		for (int i = 0; i < stringLength; i++) {
			int c = string.charAt(i);
			if (c <= 0x007F) {
				utf8Length++;
			} else if (c > 0x07FF) {
				utf8Length += 3;
			} else {
				utf8Length += 2;
			}
		}
		return utf8Length;
	}

	private static void writeChars(DataOutput out, String s, int start,
			int length) throws IOException {
		final int end = start + length;
		for (int i = start; i < end; i++) {
			int code = s.charAt(i);
			if (code <= 0x7F) {
				out.writeByte((byte) code);
			} else if (code <= 0x07FF) {
				out.writeByte((byte) (0xC0 | ((code >> 6) & 0x1F)));
				out.writeByte((byte) (0x80 | code & 0x3F));
			} else {
				out.writeByte((byte) (0xE0 | ((code >> 12) & 0X0F)));
				out.writeByte((byte) (0x80 | ((code >> 6) & 0x3F)));
				out.writeByte((byte) (0x80 | (code & 0x3F)));
			}
		}
	}
	
	public static void main(String[] args) throws UnsupportedEncodingException {
		 String url = "http://s.taobao.com/search?spm=1.7274553.1997520241.3.6qlwag&q=%D4%E7%C7%EF%D0%C2%BF%EE%CC%D7%D7%B0&refpid=420462_1006&source=tbsy&style=grid&tab=all";
		 UTF8 utf8 = new UTF8(url);
		 System.out.println(utf8.toString());

		 System.out.println(new String(url.getBytes("gb2312"), "gb2312"));
		 System.out.println(URLDecoder.decode(URLDecoder.decode("http%253a%252f%252fs.taobao.com%252fsearch%253fspm%253d1.7274553.1997520241.3.6qlwag%2526q%253d%25d4%25e7%25c7%25ef%25d0%25c2%25bf%25ee%25cc%25d7%25d7%25b0%2526refpid%253d420462_1006%2526source%253dtbsy%2526style%253dgrid%2526tab%253dall"
				 , "gb2312"), "gb2312"));
	}

}

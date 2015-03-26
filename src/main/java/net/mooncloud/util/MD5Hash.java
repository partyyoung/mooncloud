package net.mooncloud.util;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class MD5Hash implements Comparable<MD5Hash> {
	public static final int MD5_LEN = 16;

	private static ThreadLocal<MessageDigest> DIGESTER_FACTORY = new ThreadLocal<MessageDigest>() {
		@Override
		protected MessageDigest initialValue() {
			try {
				return MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				throw new RuntimeException(e);
			}
		}
	};

	private byte[] digest;

	/** Constructs an MD5Hash. */
	public MD5Hash() {
		this.digest = new byte[MD5_LEN];
	}

	/** Constructs an MD5Hash from a hex string. */
	public MD5Hash(String hex) {
		setDigest(hex);
	}

	/** Constructs an MD5Hash with a specified value. */
	public MD5Hash(byte[] digest) {
		if (digest.length != MD5_LEN)
			throw new IllegalArgumentException("Wrong length: " + digest.length);
		this.digest = digest;
	}

	/** Copy the contents of another instance into this instance. */
	public void set(MD5Hash that) {
		System.arraycopy(that.digest, 0, this.digest, 0, MD5_LEN);
	}

	/** Returns the digest bytes. */
	public byte[] getDigest() {
		return digest;
	}

	/** Construct a hash value for a byte array. */
	public static MD5Hash digest(byte[] data) {
		return digest(data, 0, data.length);
	}

	/**
	 * Create a thread local MD5 digester
	 */
	public static MessageDigest getDigester() {
		MessageDigest digester = DIGESTER_FACTORY.get();
		digester.reset();
		return digester;
	}

	/** Construct a hash value for the content from the InputStream. */
	public static MD5Hash digest(InputStream in) throws IOException {
		final byte[] buffer = new byte[4 * 1024];

		final MessageDigest digester = getDigester();
		for (int n; (n = in.read(buffer)) != -1;) {
			digester.update(buffer, 0, n);
		}

		return new MD5Hash(digester.digest());
	}

	/** Construct a hash value for a byte array. */
	public static MD5Hash digest(byte[] data, int start, int len) {
		byte[] digest;
		MessageDigest digester = getDigester();
		digester.update(data, start, len);
		digest = digester.digest();
		return new MD5Hash(digest);
	}

	/** Construct a hash value for a String. */
	public static MD5Hash digest(String string) {
		return digest(UTF8.getBytes(string));
	}

	/** Construct a hash value for a String. */
	public static MD5Hash digest(UTF8 utf8) {
		return digest(utf8.getBytes(), 0, utf8.getLength());
	}

	/** Construct a half-sized version of this MD5. Fits in a long **/
	public long halfDigest() {
		long value = 0;
		for (int i = 0; i < 8; i++)
			value |= ((digest[i] & 0xffL) << (8 * (7 - i)));
		return value;
	}

	/**
	 * Return a 32-bit digest of the MD5.
	 * 
	 * @return the first 4 bytes of the md5
	 */
	public int quarterDigest() {
		int value = 0;
		for (int i = 0; i < 4; i++)
			value |= ((digest[i] & 0xff) << (8 * (3 - i)));
		return value;
	}

	/**
	 * Returns true iff <code>o</code> is an MD5Hash whose digest contains the
	 * same values.
	 */
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof MD5Hash))
			return false;
		MD5Hash other = (MD5Hash) o;
		return Arrays.equals(this.digest, other.digest);
	}

	/**
	 * Returns a hash code value for this object. Only uses the first 4 bytes,
	 * since md5s are evenly distributed.
	 */
	@Override
	public int hashCode() {
		return quarterDigest();
	}

	/** Compares this object with the specified object for order. */
	@Override
	public int compareTo(MD5Hash that) {
		// return WritableComparator.compareBytes(this.digest, 0, MD5_LEN,
		// that.digest, 0, MD5_LEN);
		return compareBytes(this.digest, 0, MD5_LEN, that.digest, 0, MD5_LEN);
	}

	public int compareBytes(byte[] buffer1, int offset1, int length1,
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

	private static final char[] HEX_DIGITS = { '0', '1', '2', '3', '4', '5',
			'6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
	private static final char[] HEX_DIGITS_UPPERCASE = { '0', '1', '2', '3',
			'4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

	/** Returns a string representation of this object. */
	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder(MD5_LEN * 2);
		for (int i = 0; i < MD5_LEN; i++) {
			int b = digest[i];
			buf.append(HEX_DIGITS[(b >> 4) & 0xf]);
			buf.append(HEX_DIGITS[b & 0xf]);
		}
		return buf.toString();
	}

	public String toString16() {

		return toString().substring(8, 24);
	}

	public String toStringUpperCase() {

		StringBuilder buf = new StringBuilder(MD5_LEN * 2);
		for (int i = 0; i < MD5_LEN; i++) {
			int b = digest[i];
			buf.append(HEX_DIGITS_UPPERCASE[(b >> 4) & 0xf]);
			buf.append(HEX_DIGITS_UPPERCASE[b & 0xf]);
		}
		return buf.toString();
		// return toString().toUpperCase();
	}

	public String toString16UpperCase() {
		return toStringUpperCase().substring(8, 24);
		// return toString().substring(8, 24).toUpperCase();
	}

	/** Sets the digest value from a hex string. */
	public void setDigest(String hex) {
		if (hex.length() != MD5_LEN * 2)
			throw new IllegalArgumentException("Wrong length: " + hex.length());
		byte[] digest = new byte[MD5_LEN];
		for (int i = 0; i < MD5_LEN; i++) {
			int j = i << 1;
			digest[i] = (byte) (charToNibble(hex.charAt(j)) << 4 | charToNibble(hex
					.charAt(j + 1)));
		}
		this.digest = digest;
	}

	private static final int charToNibble(char c) {
		if (c >= '0' && c <= '9') {
			return c - '0';
		} else if (c >= 'a' && c <= 'f') {
			return 0xa + (c - 'a');
		} else if (c >= 'A' && c <= 'F') {
			return 0xA + (c - 'A');
		} else {
			throw new RuntimeException("Not a hex character: " + c);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println(MD5Hash.digest("ccc"));
		System.out.println(MD5Hash.digest("ccc").toStringUpperCase());
		System.out.println(MD5Hash.digest("ccc").toString16());
		System.out.println(MD5Hash.digest("ccc").toString16UpperCase());
	}
}

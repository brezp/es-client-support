package com.github.brezp.es.client.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * 把 HttpEntity的 字节内容 转 字符串 内容 工具类
 *
 */
public class InputStreamUtils {

	final static int BUFFER_SIZE = 4096;

	/**
	 * 将InputStream转换成String
	 *
	 * @param in InputStream
	 * @return String
	 * @throws Exception
	 */
	public static String InputStreamTOString(InputStream in)  {
		return InputStreamTOString(in,"ISO-8859-1");
	}

	/**
	 * 将InputStream转换成某种字符编码的String
	 *
	 * @param in
	 * @param encoding
	 * @return
	 * @throws Exception
	 */
	public static String InputStreamTOString(InputStream in, String encoding) {
		try {
			ByteArrayOutputStream outStream = new ByteArrayOutputStream();
			byte[] data = new byte[BUFFER_SIZE];
			int count = -1;
			while ((count = in.read(data, 0, BUFFER_SIZE)) != -1)
				outStream.write(data, 0, count);

			data = null;
			return new String(outStream.toByteArray(), encoding);
		}catch (Exception e){
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 将String转换成InputStream
	 *
	 * @param in
	 * @return
	 * @throws Exception
	 */
	public static InputStream StringTOInputStream(String in)  {
		try {
			ByteArrayInputStream is = new ByteArrayInputStream(in.getBytes("ISO-8859-1"));
			return is;
		}catch (Exception e){
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 将InputStream转换成byte数组
	 *
	 * @param in InputStream
	 * @return byte[]
	 * @throws IOException
	 */
	public static byte[] InputStreamTOByte(InputStream in) {
		try {
			ByteArrayOutputStream outStream = new ByteArrayOutputStream();
			byte[] data = new byte[BUFFER_SIZE];
			int count = -1;
			while ((count = in.read(data, 0, BUFFER_SIZE)) != -1)
				outStream.write(data, 0, count);

			data = null;
			return outStream.toByteArray();
		}catch (Exception e){
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 将byte数组转换成InputStream
	 *
	 * @param in
	 * @return
	 * @throws Exception
	 */
	public static InputStream byteTOInputStream(byte[] in) {
		try {
			ByteArrayInputStream is = new ByteArrayInputStream(in);
			return is;
		}catch (Exception e){
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 将byte数组转换成String
	 *
	 * @param in
	 * @return
	 * @throws Exception
	 */
	public static String byteTOString(byte[] in) throws Exception {
		try {

			InputStream is = byteTOInputStream(in);
			return InputStreamTOString(is);
		}catch (Exception e){
			e.printStackTrace();
		}
		return null;
	}
}
 

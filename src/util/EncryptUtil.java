package util;
/**
 * 项目名称：spam_cms 类 名 称：EncryptUtil 类 描 述：(描述信息) 创 建 人：linfeng 创建时间：2015年11月24日
 * 下午1:35:59 修 改 人：Lenovo 修改时间：2015年11月24日 下午1:35:59 修改备注：
 * 
 * @version
 */


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

/**
 * @包名：com.cmcc.spam.cms.comm.util
 * @类名：EncryptUtil
 * @描述：(描述这个类的作用) @作者：linfeng
 * @时间：2015年11月24日下午1:35:59 @版本：1.0.0
 */
public class EncryptUtil {
	private static final Logger logger = Logger.getLogger(EncryptUtil.class);
	
	/**
	 * 用MD5算法进行加密
	 * 
	 * @param str 需要加密的字符串
	 * @return MD5加密后的结果
	 */
	public static String encodeMD5String(String str) {
		return encode(str, "MD5");
	}
	
	/**
	 * 用SHA算法进行加密
	 * 
	 * @param str 需要加密的字符串
	 * @return SHA加密后的结果
	 */
	public static String encodeSHAString(String str) {
		return encode(str, "SHA");
	}
	
	/**
	 * 用base64算法进行加密
	 * 
	 * @param str 需要加密的字符串
	 * @return base64加密后的结果
	 */
	public static String encodeBase64String(String str) {
		try {
			Base64 base64 = new Base64();
			byte[] enbytes;
			
			enbytes = base64.encodeBase64Chunked(str.getBytes("UTF-8"));
			
			return new String(enbytes);
		}
		catch(UnsupportedEncodingException e) {
			logger.error(null, e);
			return "";
		}
	}
	
	/**
	 * 用base64算法进行解密
	 * 
	 * @param str 需要解密的字符串
	 * @return base64解密后的结果
	 * @throws IOException
	 */
	public static String decodeBase64String(String str) throws IOException {
		Base64 base64 = new Base64();
		byte[] enbytes = base64.decodeBase64(new String(str).getBytes("UTF-8"));
		return new String(enbytes);
	}
	
	private static String encode(String str, String method) {
		MessageDigest md = null;
		String dstr = null;
		try {
			md = MessageDigest.getInstance(method);
			md.update(str.getBytes());
			byte b[] = md.digest();
			
			int i;
			
			StringBuffer buf = new StringBuffer("");
			for(int offset = 0; offset < b.length; offset++) {
				i = b[offset];
				if(i < 0)
					i += 256;
				if(i < 16)
					buf.append("0");
				buf.append(Integer.toHexString(i));
			}
			// 32位加密
			return buf.toString();
		}
		catch(Exception e) {
			logger.error(null, e);
		}
		return dstr;
	}
	
//	public static void main(String[] args) throws IOException {
//		String user = "123456";
//		System.out.println("原始字符串 " + user);
//		System.out.println("MD5加密 " + encodeMD5String(user));
//	}
}

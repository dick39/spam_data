/**
 * 项目名称：spam_cms 类 名 称：UuidUtil 类 描 述：(描述信息) 创 建 人：linfeng 创建时间：2015年9月8日
 * 下午3:33:49 修 改 人：Lenovo 修改时间：2015年9月8日 下午3:33:49 修改备注：
 * 
 * @version
 */


import java.util.UUID;

/**
 * @包名：com.cmcc.spam.cms.comm.util
 * @类名：UuidUtil
 * @描述：(描述这个类的作用)
 * @作者：linfeng
 * @时间：2015年9月8日下午3:33:49
 * @版本：1.0.0
 */
public class UuidUtil {
	public static String getUuid() {
		String uuid = UUID.randomUUID().toString();
		uuid = uuid.replace("-", "");
		return uuid;
	}
}

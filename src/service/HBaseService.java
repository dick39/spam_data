/**
 * 项目名称：HbaseTest 类 名 称：HBaseService 类 描 述：(描述信息) 创 建 人：Zero 创建时间：2016年6月17日
 * 上午8:53:45 修 改 人：Zero 修改时间：2016年6月17日 上午8:53:45 修改备注：
 * 
 * @version
 */
package service;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import util.EncryptUtil;
import util.HBaseConstant;
import util.HBaseUtil;
import util.UuidUtil;

/**
 * @包名：service
 * @类名：HBaseService
 * @描述：(描述这个类的作用) @作者：Zero
 * @时间：2016年6月17日上午8:53:45 @版本：1.0.0
 */
public class HBaseService {
	public void createTestData() {
		String[] cols = {"msgId", "userId", "sendId", "date", "md5", "contentType", "contentLength", "content", "filePath", "filterResult",
				"policyType", "policyDetail", "policyId", "svcType" };
				
		long count = 1000000;
		List<String[]> valList = new ArrayList<String[]>();
		
		for(int i = 0; i < count; i++) {
			Random r = new Random();
			String msgId = UuidUtil.getUuid();
			String md5 = EncryptUtil.encodeMD5String(r.nextInt(10000) + "");
			String[] values = {msgId, "2", "3", "4", md5, "6", "7", "8", "9", "10", "11", "12", "13", "14" };
			valList.add(values);
		}
		
		// 生成测试数据
		HBaseUtil.insertTest(count, HBaseConstant.TABLE_NAME, HBaseConstant.COLUMN_FAMILY_NAME, cols, valList);
		System.out.println(HBaseUtil.rowCount(HBaseConstant.TABLE_NAME, HBaseConstant.COLUMN_FAMILY_NAME));
		HBaseUtil.scanAllRecord(HBaseConstant.TABLE_NAME);
	}
	
	// 根据热点消息的md5,查询出所有发送过该消息的用户id
	public static void selectHotspotUserIds(String hotspotMsgMd5, String... columns) {
		HBaseUtil.scanRecord(HBaseConstant.TABLE_NAME, hotspotMsgMd5, hotspotMsgMd5 + "~", HBaseConstant.COLUMN_FAMILY_NAME, columns);
	}
	
	public static void main(String[] args) {
		selectHotspotUserIds("00003e3b9e5336685200ae85d21b4f5e");
	}
}

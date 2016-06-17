/**
 * 项目名称：Spam_DataAnalysis 类 名 称：MsgPo 类 描 述：(描述信息) 创 建 人：Zero 创建时间：2016年4月29日
 * 上午9:19:21 修 改 人：Zero 修改时间：2016年4月29日 上午9:19:21 修改备注：
 * 
 * @version
 */
package po;

/**
 * @包名：com.cmcc.po
 * @类名：MsgPo
 * @描述：(描述这个类的作用) @作者：Zero
 * @时间：2016年4月29日上午9:19:21 @版本：1.0.0
 */
public class MsgPo {
	
	public MsgPo() {
	}
	
	public MsgPo(String poStr) {
		String[] poStrArray = poStr.split("\\|");
		if(poStrArray.length == 18) {
			this.setMsgId(poStrArray[0]);
			this.setSendId(poStrArray[1]);
			this.setRevcId(poStrArray[2]);
			this.setDate(poStrArray[3]);
			this.setMd5(poStrArray[4]);
			this.setContenttype(Integer.parseInt(poStrArray[5]));
			this.setContentlength(Integer.parseInt(poStrArray[6]));
			this.setContent(poStrArray[7]);
			this.setFilePath(poStrArray[8]);
			this.setFilterResult(Integer.parseInt(poStrArray[9]));
			this.setPolicyType(poStrArray[10]);
			this.setPolicyDetail(poStrArray[11]);
			this.setPolicyId(poStrArray[12]);
			this.setSvc_type(Integer.parseInt(poStrArray[13]));
			this.setClassify(Integer.parseInt(poStrArray[14]));
			this.setFingerprintType(Integer.parseInt(poStrArray[15]));
			this.setCredit(Integer.parseInt(poStrArray[16]));
			this.setFilterPath(Integer.parseInt(poStrArray[17]));
			
			this.setSendCount(poStrArray[1], poStrArray[2]);
			this.setIntercept(poStrArray[9]);
		}
	}
	
	// 消息的id
	private String msgId;
	// 发送方号码
	private String sendId;
	// 接收方号码
	private String revcId;
	// 发送时间
	private String date;
	// 消息的md5
	private String md5;
	// 消息类型：1、文本 2、图片 3、音频 4、视频 5、文件 6、网址
	private int contenttype;
	// 消息长度（字节长度）
	private int contentlength;
	// 消息内容
	private String content;
	// 消息存放的路径
	private String filePath;
	// 消息过滤结果：1、未中策略放行 2、放行 3、先发后审 4、先审后发 5、拦截不送审 6、拦截送审 7、拦截加黑送审
	private int filterResult;
	// 命中的策略类型：1、关键词策略 2、无用（原：指纹策略）3、无用（原：人工审核结果指纹）
	// 4、用户频次策略 5、无用（原：人工审核结果） 6、富媒体策略 7、无用（原：应急管控） 8、无用（原：黑名单） 9、热点送审策略
	// 10、黑指纹策略 11、白指纹 12、人工审核临时黑指纹 13、人工审核临时白指纹 14、黑名单生成策略 20、业管一级关键词 21、业管黑名单
	// 22、业管白名单 23、业管用户分级频次 25、群黑名单
	private int policyType;
	// 命中的策略详情：
	private String policyDetail;
	// 命中的策略id：
	private String policyId;
	// 业务类型：1、点对点 2、群发 3、群聊 4、圈子 5、profile 6、公众号
	private int svc_type;
	// 内容类型：0、未分类 1、政治 2、涉黄 3、诈骗 4、广告 5、涉黑 6、其他
	private int classify;
	// 是否命中黑指纹： 0、未命中 1、命中
	private int fingerprintType;
	// 信用等级： 1、1级 2、2级 3、3级 4、4级 5、5级
	private int credit;
	// 消息流经的策略（32位bit转成的整数，从右侧开始计数，1表示流经该策略）：
	// 1、关键词策略 2、无用（原：指纹策略）3、无用（原：人工审核结果指纹）
	// 4、用户频次策略 5、无用（原：人工审核结果） 6、富媒体策略 7、无用（原：应急管控） 8、无用（原：黑名单） 9、热点送审策略
	// 10、黑指纹策略 11、白指纹 12、人工审核临时黑指纹 13、人工审核临时白指纹 14、黑名单生成策略 20、业管一级关键词 21、业管黑名单
	// 22、业管白名单 23、业管用户分级频次 24、业管群黑名单
	private int filterPath;
	
	// 以下是通过计算得到的字段
	private int sendCount;
	//
	private int intercept;
	
	/**
	 * @return the msgId
	 */
	public String getMsgId() {
		return msgId;
	}
	
	/**
	 * @param msgId the msgId to set
	 */
	public void setMsgId(String msgId) {
		this.msgId = msgId;
	}
	
	/**
	 * @return the sendId
	 */
	public String getSendId() {
		return sendId;
	}
	
	/**
	 * @param sendId the sendId to set
	 */
	public void setSendId(String sendId) {
		if(null != sendId && sendId.startsWith("tel:+86")) {
			sendId = sendId.substring(7);
		}
		else
			if(sendId.startsWith("+86")) {
				sendId = sendId.substring(3);
			}
		this.sendId = sendId;
	}
	
	/**
	 * @return the revcId
	 */
	public String getRevcId() {
		return revcId;
	}
	
	/**
	 * @param revcId the revcId to set
	 */
	public void setRevcId(String revcId) {
		this.revcId = revcId;
	}
	
	/**
	 * @return the date
	 */
	public String getDate() {
		return date;
	}
	
	/**
	 * @param date the date to set
	 */
	public void setDate(String date) {
		this.date = date;
	}
	
	/**
	 * @return the md5
	 */
	public String getMd5() {
		return md5;
	}
	
	/**
	 * @param md5 the md5 to set
	 */
	public void setMd5(String md5) {
		this.md5 = md5;
	}
	
	/**
	 * @return the contenttype
	 */
	public int getContenttype() {
		return contenttype;
	}
	
	/**
	 * @param contenttype the contenttype to set
	 */
	public void setContenttype(int contenttype) {
		this.contenttype = contenttype;
	}
	
	/**
	 * @return the contentlength
	 */
	public int getContentlength() {
		return contentlength;
	}
	
	/**
	 * @param contentlength the contentlength to set
	 */
	public void setContentlength(int contentlength) {
		this.contentlength = contentlength;
	}
	
	/**
	 * @return the filterResult
	 */
	public int getFilterResult() {
		return filterResult;
	}
	
	/**
	 * @param filterResult the filterResult to set
	 */
	public void setFilterResult(int filterResult) {
		this.filterResult = filterResult;
	}
	
	/**
	 * @return the policyType
	 */
	public int getPolicyType() {
		return policyType;
	}
	
	/**
	 * @param policyType the policyType to set
	 */
	public void setPolicyType(String policyType) {
		if(null == policyType || "".equals(policyType.trim())) {
			this.policyType = 0;
		}
		else {
			this.policyType = Integer.parseInt(policyType.split(",")[0].trim());
		}
	}
	
	/**
	 * @return the svc_type
	 */
	public int getSvc_type() {
		return svc_type;
	}
	
	/**
	 * @param svc_type the svc_type to set
	 */
	public void setSvc_type(int svc_type) {
		this.svc_type = svc_type;
	}
	
	/**
	 * @return the classify
	 */
	public int getClassify() {
		return classify;
	}
	
	/**
	 * @param classify the classify to set
	 */
	public void setClassify(int classify) {
		this.classify = classify;
	}
	
	/**
	 * @return the fingerprintType
	 */
	public int getFingerprintType() {
		return fingerprintType;
	}
	
	/**
	 * @param fingerprintType the fingerprintType to set
	 */
	public void setFingerprintType(int fingerprintType) {
		this.fingerprintType = fingerprintType;
	}
	
	/**
	 * @return the credit
	 */
	public int getCredit() {
		return credit;
	}
	
	/**
	 * @param credit the credit to set
	 */
	public void setCredit(int credit) {
		// 由于原始的信用等级从0开始，所以对其进行+1的操作
		this.credit = credit + 1;
	}
	
	/**
	 * @param intercept the intercept to set
	 */
	public void setIntercept(int intercept) {
		this.intercept = intercept;
	}
	
	/**
	 * @return the content
	 */
	public String getContent() {
		return content;
	}
	
	/**
	 * @param content the content to set
	 */
	public void setContent(String content) {
		this.content = content;
	}
	
	/**
	 * @return the filePath
	 */
	public String getFilePath() {
		return filePath;
	}
	
	/**
	 * @param filePath the filePath to set
	 */
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	
	/**
	 * @return the policyDetail
	 */
	public String getPolicyDetail() {
		return policyDetail;
	}
	
	/**
	 * @param policyDetail the policyDetail to set
	 */
	public void setPolicyDetail(String policyDetail) {
		this.policyDetail = policyDetail;
	}
	
	/**
	 * @return the policyId
	 */
	public String getPolicyId() {
		return policyId;
	}
	
	/**
	 * @param policyId the policyId to set
	 */
	public void setPolicyId(String policyId) {
		this.policyId = policyId;
	}
	
	/**
	 * @return the filterPath
	 */
	public int getFilterPath() {
		return filterPath;
	}
	
	/**
	 * @param filterPath the filterPath to set
	 */
	public void setFilterPath(int filterPath) {
		this.filterPath = filterPath;
	}
	
	/**
	 * @return the sendCount
	 */
	public int getSendCount() {
		return sendCount;
	}
	
	/**
	 * @param sendCount the sendCount to set
	 */
	public void setSendCount(String sendId, String resvId) {
		if(null == sendId || "".equals(sendId.trim())) {
			this.sendCount = 0;
		}
		else
			if(null == resvId || "".equals(resvId.trim())) {
				this.sendCount = 1;
			}
			else {
				this.sendCount = resvId.split(",").length;
			}
	}
	
	/**
	 * @return the intercept
	 */
	public int getIntercept() {
		return intercept;
	}
	
	/**
	 * @param intercept the intercept to set
	 */
	public void setIntercept(String filterResult) {
		// 统计自动拦截的消息
		if(null == filterResult) {
			this.intercept = 0;
		}
		else
			if(filterResult.equals("5") || filterResult.equals("6") || filterResult.equals("7")) {
				this.intercept = this.sendCount;
			}
	}
	
}

package cn.com.analysys.javasdk;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * @author admin
 */
public class LogCollecter implements Collecter {
	private final String logFolder;
	private final SimpleDateFormat format;
	private boolean async = false;
	private final static int DEFAULT_BATCH_NUM = 20;
	private final static long DEFAULT_BATCH_SEC = 10;
	private long sendTimer = -1;
	private List<Map<String, Object>> batchMsgList;
	private ExecutorService singleThread;
	private boolean isListen = true;
	private int batchNum;
	private long batchSec;
	
	public LogCollecter(String logFolder){
		this(logFolder, GeneralRule.HOUR, false, DEFAULT_BATCH_NUM, DEFAULT_BATCH_SEC);
	}
	
	public LogCollecter(String logFolder, boolean async){
		this(logFolder, GeneralRule.HOUR, async, DEFAULT_BATCH_NUM, DEFAULT_BATCH_SEC);
	}
	
	public LogCollecter(String logFolder, GeneralRule rule){
		this(logFolder, rule, false, DEFAULT_BATCH_NUM, DEFAULT_BATCH_SEC);
	}
	
	public LogCollecter(String logFolder, GeneralRule rule, boolean async){
		this(logFolder, rule, async, DEFAULT_BATCH_NUM, DEFAULT_BATCH_SEC);
	}
	
	public LogCollecter(String logFolder, GeneralRule rule, boolean async, int batchNum, long batchSec){
		this.logFolder = logFolder;
		if(GeneralRule.DAY.equals(rule)){
    		this.format = new SimpleDateFormat("yyyyMMdd");
    	} else {
    		this.format = new SimpleDateFormat("yyyyMMddHH");
    	}
		this.async = async;
		this.batchMsgList = new ArrayList<Map<String, Object>>(1);
		if(this.async){
			this.batchNum = batchNum;
			this.batchSec = batchSec * 1000;
			this.batchMsgList = new ArrayList<Map<String, Object>>(this.batchNum);
			this.singleThread = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
			init();
		}
	}

	@Override
	public boolean send(Map<String, Object> egCollectMessage) {
		try {
			if(!async){
				batchMsgList.add(egCollectMessage);
				upload();
			} else {
				synchronized (batchMsgList) {
					if(sendTimer == -1)
						sendTimer = System.currentTimeMillis();
					batchMsgList.add(egCollectMessage);
			    }
				if (batchMsgList.size() >= batchNum) {
					upload();
				}
			}
		} catch (Exception e) {
			System.out.println("Log Data Error: " + e);
		}
		return false;
	}

	@Override
	public void upload() {
		synchronized (batchMsgList) {
			if(batchMsgList != null && batchMsgList.size() > 0){
				String jsonData;
				try {
					jsonData = ValidHandle.getEgJsonMapper().writeValueAsString(batchMsgList);
					boolean success = LogWriter.write(logFolder, generalNowTime(), jsonData.concat("\n"));
					if(!success) {
						int total = 3; //重试3次
						while(!success && total-- > 0){
							try { Thread.sleep(1000); } catch (Exception e1) {System.out.println(e1);}
							success = LogWriter.write(logFolder, generalNowTime(), jsonData.concat("\n"));
						}
					}
				} catch (JsonProcessingException e) {
					System.out.println("Json Serialize Error: " + e);
				} finally {
					batchMsgList.clear();
					if(this.async)
						resetTimer();
				}
			}
		}
	}
	
	@Override
	public void flush() {
		upload();
	}
	
	@Override
	public void close() {
		flush();
		shutdown();
	}

	@Override
	public void debug(boolean debug) {}
	
	private void init(){
		this.singleThread.execute(new Runnable() {
			@Override
			public void run() {
				while(isListen){
					try { Thread.sleep(1000); } catch (Exception e1) {System.out.println(e1);}
					if (sendTimer != -1 && (System.currentTimeMillis() - sendTimer >= batchSec)) {
						try {
							upload();
						} catch (Exception e) {}
					}
				}
			}
		});
	}
	
	private void shutdown(){
		this.isListen = false;
		try {
			if(this.async){
				this.singleThread.shutdown();
				this.singleThread.awaitTermination(5, TimeUnit.SECONDS);
			}
		} catch (Exception e) {
			System.out.println(e);
			this.singleThread = null;
		}
	}
	
	private void resetTimer(){
		this.sendTimer = -1;
	}
	
	public String generalNowTime() {
		return format.format(new Date());
    }
    
    public static enum GeneralRule {
    	HOUR, DAY;
    }
}

package cn.com.analysys.javasdk;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author admin
 */
public class LogWriter {
	private static volatile Map<String, FileOutputStream> streamCachedMap = new HashMap<String, FileOutputStream>();
	private static volatile Map<String, AtomicInteger> streamNumberMap = new HashMap<String, AtomicInteger>();
	private static final int MIN_CATCH_NUM = 1;
	private static final String FILENAME_TEMPLET = "datas_$DATEPATTEN.log";
	
    public static boolean write(String logFolder, String logFileTime, final String value, FileOutputStream lockstream) throws IOException {
		FileOutputStream stream = getFileStream(logFolder, logFileTime);
		streamNumberMap.get(logFileTime).incrementAndGet();
		if(stream != null){
    		synchronized (stream) {
    			if(!streamCachedMap.containsKey(logFileTime)){
    				return false;
    			}
    			FileLock lock = null;
    	        try {
    	        	FileChannel channel = null;
    	        	if(lockstream != null){
    	        		channel = lockstream.getChannel();
    	        	} else {
    	        		channel = stream.getChannel();
    	        	}
    	        	if(channel == null || !channel.isOpen()){
    	        		return false;
    	        	}
    				lock = channel.lock();
    	        	stream.write(value.getBytes("UTF-8"));
    	        	return true;
    	        } catch (IOException e) {
    	        	throw e;
    	        } finally {
    	        	streamNumberMap.get(logFileTime).decrementAndGet();
    	        	if(lock != null)
    					try {
    						lock.release();
    					} catch (IOException e) {
    						throw e;
    					}
    	        }
    		}
		}
        return false;
    }
    
    private static FileOutputStream getFileStream(String logFolder, String logFileTime) {
    	if(!logFolder.endsWith(File.separator))
    		logFolder = logFolder.concat(File.separator);
    	if(!new File(logFolder).exists())
    		new File(logFolder).mkdirs();
    	String logFilePath = logFolder.concat(FILENAME_TEMPLET.replaceAll("\\$DATEPATTEN", logFileTime));
    	synchronized (streamCachedMap) {
    		if(streamCachedMap.size() > MIN_CATCH_NUM){
				removeStream(logFileTime);
			}
    		if(!streamCachedMap.containsKey(logFileTime)){
    			FileOutputStream out;
				try {
					out = new FileOutputStream(logFilePath, true);
					streamCachedMap.put(logFileTime, out);
					streamNumberMap.put(logFileTime, new AtomicInteger(0));
				} catch (FileNotFoundException e) {
					out = null;
				}
    		}
    		return streamCachedMap.get(logFileTime);
    	}
    }
    
    private static void removeStream(String logFileTime) {
    	for(Iterator<Map.Entry<String, FileOutputStream>> it = streamCachedMap.entrySet().iterator(); it.hasNext();){
			Map.Entry<String, FileOutputStream> property = it.next();
			try {
				long cachTime = Long.valueOf(property.getKey());
				long requestTime = Long.valueOf(logFileTime);
	    		if(requestTime - cachTime >= MIN_CATCH_NUM && (streamNumberMap.containsKey(property.getKey()) && streamNumberMap.get(property.getKey()).get() == 0)){
	    			property.getValue().close();
	    			it.remove();
	    			streamNumberMap.remove(property.getKey());
	    		}
			} catch (Exception e) {}
    	}
    }
    
    public static String monitor() {
		StringBuffer bufer = new StringBuffer();
		bufer.append(String.format("Stream Catch Status: %s:%s", streamCachedMap.size(), streamNumberMap.size()));
		for(Iterator<Map.Entry<String, FileOutputStream>> it = streamCachedMap.entrySet().iterator(); it.hasNext();){
			Map.Entry<String, FileOutputStream> property = it.next();
			bufer.append(String.format("    date[%s] num[%s]", property.getKey(), streamNumberMap.get(property.getKey())));
		}
        return bufer.toString();
	}
    
    public static void close(){
    	for(Iterator<Map.Entry<String, FileOutputStream>> it = streamCachedMap.entrySet().iterator(); it.hasNext();){
			Map.Entry<String, FileOutputStream> property = it.next();
			try {
				if(property.getValue() != null)
					property.getValue().close();
			} catch (Exception e) {}
		}
	}
}

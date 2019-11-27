package cn.com.analysys.javasdk;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author admin
 */
public class LogWriter {
	private static volatile Map<String, FileOutputStream> streamCachedMap = new HashMap<String, FileOutputStream>();

    public static boolean write(String logFolder, String logFileTime, final String value) {
		FileOutputStream stream = getFileStream(logFolder, logFileTime);
		if(stream != null){
    		synchronized (stream) {
    			FileLock lock = null;
    	        try {
    				lock = stream.getChannel().lock();
    	        	stream.write(value.getBytes("UTF-8"));
    	        	return true;
    	        } catch (Exception e) {
    	            throw new RuntimeException("Write file error.", e);
    	        } finally {
    	        	if(lock != null)
    					try {
    						lock.release();
    					} catch (IOException e) {
    						throw new RuntimeException("Release file lock error.", e);
    					}
    	        }
    		}
		}
        return false;
    }
    
    private static FileOutputStream getFileStream(String logFolder, String logFileTime) {
    	if(!logFolder.endsWith("/"))
    		logFolder = logFolder.concat("/");
    	if(!new File(logFolder).exists())
    		new File(logFolder).mkdirs();
    	String logFilePath = logFolder.concat("datas_").concat(logFileTime).concat(".log");
    	synchronized (streamCachedMap) {
    		if(!streamCachedMap.containsKey(logFileTime)){
    			FileOutputStream out;
				try {
					out = new FileOutputStream(logFilePath, true);
					streamCachedMap.put(logFileTime, out);
				} catch (FileNotFoundException e) {
					out = null;
				}
    			if(streamCachedMap.size() > 1){
    				removeStream(logFileTime);
    			}
    		}
    	}
		return streamCachedMap.get(logFileTime);
    }
    
    private static void removeStream(String logFileTime) {
    	for(Iterator<Map.Entry<String, FileOutputStream>> it = streamCachedMap.entrySet().iterator(); it.hasNext();){
			Map.Entry<String, FileOutputStream> property = it.next();
			try {
				long cachTime = Long.valueOf(property.getKey());
				long requestTime = Long.valueOf(logFileTime);
	    		if(cachTime < requestTime){
	    			property.getValue().close();
	    			it.remove();
	    		}
			} catch (Exception e) {}
    	}
    }
}

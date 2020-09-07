package cn.com.analysys.javasdk;

public class AnalysysLogger {
	private static AnalysysJavaSdkLog logger;
	
	public static void print(String msg){
		if(logger != null){
			logger.print(String.format("[AnalysysJavaSdk]: %s", msg));
		} else {
			System.out.println(String.format("[AnalysysJavaSdk]: %s", msg));
		}
	}

	public static void setLogger(AnalysysJavaSdkLog logger) {
		AnalysysLogger.logger = logger;
	}

}

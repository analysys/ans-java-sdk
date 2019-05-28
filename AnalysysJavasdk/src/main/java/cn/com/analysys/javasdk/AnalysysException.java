package cn.com.analysys.javasdk;

/**
 * @author admin
 */
public class AnalysysException extends Exception {

    public AnalysysException(String message) {
        super(message);
    }

    public AnalysysException(Throwable error) {
        super(error);
    }

}

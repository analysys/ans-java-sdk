package cn.com.analysys.javasdk;

/**
 * @author admin
 */
public enum DEBUG {
    //0
    CLOSE(0),
    //1
    OPENNOSAVE(1),
    //2
    OPENANDSAVE(2);
    private int code;

    private DEBUG(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}

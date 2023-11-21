package simpledb.core.exception;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/11/20
 */
public class CycleDetectedException extends RuntimeException {

    public CycleDetectedException(String message) {
        super(message);
    }

    public CycleDetectedException(String message, Throwable cause) {
        super(message, cause);
    }

    public CycleDetectedException(Throwable cause) {
        super(cause);
    }
}

package exception;

import org.apache.commons.lang3.exception.ContextedRuntimeException;
import org.apache.commons.lang3.exception.ExceptionContext;

public class DumpException extends ContextedRuntimeException {

    private String errorCode;
    private String errorDesc;

    public DumpException(String message) {
        super(message);
    }

    public DumpException(String errorCode, String errorDesc) {
        this(errorCode + " : " + errorDesc);
    }

    public DumpException(Throwable cause) {
        super(cause);
    }

    public DumpException(String message, Throwable cause) {
        super(message, cause);
    }

    public DumpException(String errorCode, String errorDesc, Throwable cause) {
        super(errorCode + " : " + errorDesc, cause);
    }

    public DumpException(String message, Throwable cause, ExceptionContext context) {
        super(message, cause, context);
    }

    public DumpException(String errorCode, String errorDesc, Throwable cause, ExceptionContext context) {
        super(errorCode + " : " + errorDesc, cause, context);
    }

}

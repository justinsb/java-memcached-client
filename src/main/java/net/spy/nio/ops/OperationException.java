package net.spy.nio.ops;

import java.io.IOException;


/**
 * Exceptions thrown when protocol errors occur.
 */
public final class OperationException extends IOException {
	private static final long serialVersionUID = 1L;

	private final OperationErrorType type;

	private final int errorCode;
	
	/**
	 * General exception (no message).
	 */
	public OperationException() {
		super();
		type=OperationErrorType.GENERAL;
		this.errorCode = 0;
	}

	/**
	 * Exception with a message.
	 *
	 * @param eType the type of error that occurred
	 * @param msg the error message
	 */
	public OperationException(OperationErrorType eType, String msg) {
		super(msg);
		type=eType;
		this.errorCode = 0;
	}

	public OperationException(OperationErrorType eType, int errorCode) {
		super();
		this.type=eType;
		this.errorCode = errorCode;
	}

	/**
	 * Get the type of error.
	 */
	public OperationErrorType getType() {
		return type;
	}

	@Override
	public String toString() {
		String rv=null;
		if(type == OperationErrorType.GENERAL) {
			rv="OperationException: " + type;
		} else {
			rv="OperationException: " + type + ": " + getMessage();
		}
		if (errorCode != 0) {
			rv += " ErrorCode=" + errorCode;
		}
		return rv;
	}

	public int getErrorCode() {
		return errorCode;
	}
}

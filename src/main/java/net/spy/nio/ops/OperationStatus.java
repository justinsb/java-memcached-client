package net.spy.nio.ops;

/**
 * Status indicator.
 */
public class OperationStatus {
	private final boolean success;
	private final String message;
	private final int errorCode;

	public OperationStatus(boolean success, String message) {
		this(success, message, 0);
	}

	public OperationStatus(boolean success, String message, int errorCode) {
		super();
		this.success = success;
		this.errorCode = errorCode;
		this.message = message;
	}

	/**
	 * Does this status indicate success?
	 */
	public boolean isSuccess() {
		return success;
	}

	/**
	 * Get the message included as part of this status.
	 */
	public String getMessage() {
		return message;
	}

	public int getErrorCode() {
		return errorCode;
	}

	@Override
	public String toString() {
		return "{OperationStatus success=" + success + ":  " + message + (errorCode != 0 ? " code=" + errorCode : "") + "}";
	}
}

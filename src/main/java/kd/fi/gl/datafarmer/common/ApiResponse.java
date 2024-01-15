package kd.fi.gl.datafarmer.common;

/**
 * Description:  common response
 *
 * @author ysj
 * @date 2024/1/15
 */

public class ApiResponse<T> {
    private int code;
    private String message;
    private T data;

    public ApiResponse(int code, String message, T data) {
        this.code = code;
        this.message = message;
        this.data = data;
    }

    public static <T> ApiResponse<T> success(T data) {
        return new ApiResponse<>(ResultCode.SUCCESS.getCode(), ResultCode.SUCCESS.getMessage(), data);
    }

    public static <T> ApiResponse<T> failed(String message) {
        return new ApiResponse<>(ResultCode.FAILED.getCode(), message, null);
    }

    public static <T> ApiResponse<T> failed() {
        return new ApiResponse<>(ResultCode.FAILED.getCode(), ResultCode.FAILED.getMessage(), null);
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}

package kd.fi.gl.datafarmer.common.exception;

import kd.fi.gl.datafarmer.common.ApiResponse;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.sql.SQLException;

/**
 * Description: global exception handler
 *
 * @author ysj
 * @date 2024/1/15
 */

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(DatabaseNotInitializedException.class)
    public ApiResponse<String> handleDatabaseNotInitializeException(DatabaseNotInitializedException e) {
        return ApiResponse.failed("数据库未正确初始化，请先进行数据库连接配置");
    }


    @ExceptionHandler(SQLException.class)
    public ApiResponse<String> handleExceptionHandler(SQLException e) {
        return ApiResponse.failed("数据库执行异常：" + e.getMessage());
    }

    @ExceptionHandler(ParseConfigException.class)
    public ApiResponse<String> handleParseConfigException(ParseConfigException e) {
        return ApiResponse.failed("配置解析异常：" + e.getMessage());
    }
    @ExceptionHandler(IllegalArgumentException.class)
    public ApiResponse<String> handleIllegalArgumentException(IllegalArgumentException e) {
        return ApiResponse.failed("参数异常：" + e.getMessage());
    }

    @ExceptionHandler(RuntimeException.class)
    public ApiResponse<String> handleRuntimeException(RuntimeException e) {
        return ApiResponse.failed("未知异常：" + e.getMessage());
    }

}

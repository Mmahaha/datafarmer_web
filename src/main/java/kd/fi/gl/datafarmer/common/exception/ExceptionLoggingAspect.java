package kd.fi.gl.datafarmer.common.exception;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

@Aspect
@Component
@Slf4j
public class ExceptionLoggingAspect {

    @After("execution(* kd.fi.gl.datafarmer.common.exception.GlobalExceptionHandler.*(..))")
    public void logException(JoinPoint joinPoint) {
        Object arg = joinPoint.getArgs()[0];
        if (arg instanceof Exception) {
            log.error("监听到异常信息：", ((Exception) arg));
        }
    }
}

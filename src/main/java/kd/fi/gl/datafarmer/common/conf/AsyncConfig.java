package kd.fi.gl.datafarmer.common.conf;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 * Description:
 *
 * @author ysj
 * @date 2024/1/17
 */

@Configuration
@EnableAsync
public class AsyncConfig implements AsyncConfigurer {
}

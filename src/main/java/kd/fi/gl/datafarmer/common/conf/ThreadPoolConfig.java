package kd.fi.gl.datafarmer.common.conf;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Description:
 *
 * @author ysj
 * @date 2024/1/17
 */

@Configuration
@EnableAsync
public class ThreadPoolConfig implements AsyncConfigurer {

    @Bean(name = "dataFarmerTaskExecutor")
    public ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(12);
        executor.setMaxPoolSize(12);
//        executor.setTaskDecorator();
        return executor;
    }

}

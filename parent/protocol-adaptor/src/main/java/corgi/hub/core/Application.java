package corgi.hub.core;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
@EnableDiscoveryClient
@EnableScheduling
@ComponentScan("corgi.hub.core")
public class Application {
    public static ConfigurableApplicationContext context;
    public static String nodeId;

    public void init(String []args) throws Exception {
        String configName = System.getProperty("hub.config.name");
        configName = configName == null ? "corgi" : configName;
        System.setProperty("spring.config.name", configName);
        Application.context = SpringApplication.run(Application.class, args);
    }

    public static ConfigurableApplicationContext getContenxt() {
        return context;
    }
    
	public static void main(String []args) throws Exception {
	    new Application().init(args);
	}
	
	@LoadBalanced
    @Bean
    RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @Bean(name = "queueThreadPoolExecutor")
    public ThreadPoolTaskExecutor createThreadPoolTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(100);
        executor.setMaxPoolSize(100);
        executor.setQueueCapacity(1);
        executor.setKeepAliveSeconds(300);
        executor.initialize();
        return executor;
    }
}

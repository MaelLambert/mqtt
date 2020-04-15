package cn.ciyelc.mqtt;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication(scanBasePackages = {"cn.ciyelc.mqtt"})
public class MqttServerApplication {
    public static void main(String[] args) {
        SpringApplication.run(MqttServerApplication.class, args);
    }
}
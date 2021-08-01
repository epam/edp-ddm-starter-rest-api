package com.epam.digital.data.platform.starter.restapi.config;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GenericConfig {

    @Bean
    public Clock clock() {
        return Clock.systemDefaultZone();
    }
}

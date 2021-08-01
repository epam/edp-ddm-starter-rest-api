package com.epam.digital.data.platform.starter.restapi;

import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

@Configuration
@ComponentScan
@AutoConfigureOrder(Ordered.HIGHEST_PRECEDENCE)
public class RestApiAutoConfiguration {
}

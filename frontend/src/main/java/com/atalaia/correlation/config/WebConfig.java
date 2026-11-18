package com.atalaia.correlation.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/resources/images/users/**")
                .addResourceLocations(
                        "file:/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/resources/images/users/",
                        "file:/home/jcolinm/.build-cache/atalaia-frontend/target/classes/META-INF/resources/resources/images/users/",
                        "classpath:/META-INF/resources/resources/images/users/"
                );
    }
}

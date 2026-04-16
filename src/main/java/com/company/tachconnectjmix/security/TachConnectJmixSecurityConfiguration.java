package com.company.tachconnectjmix.security;

import io.jmix.core.JmixSecurityFilterChainOrder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
public class TachConnectJmixSecurityConfiguration {

    @Bean
    @Order(JmixSecurityFilterChainOrder.CUSTOM)
    SecurityFilterChain publicFilterChain(HttpSecurity http) throws Exception {
        http.securityMatcher("/public/**", "/api/metadata/**")
                .authorizeHttpRequests(authorize ->
                        authorize.anyRequest().permitAll()
            )
            .csrf(csrf -> csrf.ignoringRequestMatchers("/api/metadata/**"));

        return http.build();
    }
}
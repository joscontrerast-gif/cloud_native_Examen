package com.lumina.bff.controller;

import com.lumina.bff.config.CircuitBreakerHelper;
import com.lumina.bff.config.MicroservicesConfig;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

/** Reenvía el pago simulado del frontend a ms-pagos (requiere sesión iniciada). */
@RestController
@RequestMapping("/api/pagos")
public class PagosSimulacionProxyController {

    private final WebClient.Builder webClientBuilder;
    private final MicroservicesConfig config;
    private final CircuitBreakerHelper circuitBreaker;

    public PagosSimulacionProxyController(WebClient.Builder webClientBuilder,
                                          MicroservicesConfig config,
                                          CircuitBreakerHelper circuitBreaker) {
        this.webClientBuilder = webClientBuilder;
        this.config = config;
        this.circuitBreaker = circuitBreaker;
    }

    @PostMapping("/simular")
    public Mono<ResponseEntity<String>> simularPago(@RequestBody String body) {
        return circuitBreaker.protect("pagos", webClientBuilder.build()
                .post()
                .uri(config.getPagosUrl() + "/api/pagos/simular")
                .bodyValue(body)
                .header("Content-Type", "application/json")
                .retrieve()
                .toEntity(String.class)
                .onErrorResume(WebClientResponseException.class, ex ->
                        Mono.just(ResponseEntity.status(ex.getStatusCode())
                                .header("Content-Type", "application/json")
                                .body(ex.getResponseBodyAsString()))));
    }
}

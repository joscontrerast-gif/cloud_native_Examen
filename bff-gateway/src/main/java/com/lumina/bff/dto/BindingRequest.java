package com.lumina.bff.dto;

import jakarta.validation.constraints.NotBlank;

/**
 * Cuerpo de POST /api/admin/bindings.
 *
 * @param queue      cola destino
 * @param exchange   exchange origen
 * @param routingKey routing key (puede ir vacía en un exchange fanout)
 */
public record BindingRequest(
        @NotBlank(message = "La cola es obligatoria")
        String queue,

        @NotBlank(message = "El exchange es obligatorio")
        String exchange,

        String routingKey
) {
}

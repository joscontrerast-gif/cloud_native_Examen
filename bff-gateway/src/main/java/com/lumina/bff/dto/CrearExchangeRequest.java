package com.lumina.bff.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/**
 * Cuerpo de POST /api/admin/exchanges.
 *
 * @param name nombre del exchange
 * @param type direct, topic, fanout o headers
 */
public record CrearExchangeRequest(
        @NotBlank(message = "El nombre del exchange es obligatorio")
        String name,

        @NotBlank(message = "El tipo de exchange es obligatorio")
        @Pattern(regexp = "(?i)direct|topic|fanout|headers",
                message = "El tipo debe ser direct, topic, fanout o headers")
        String type
) {
}

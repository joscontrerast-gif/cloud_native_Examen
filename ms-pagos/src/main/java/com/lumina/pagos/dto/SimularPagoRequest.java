package com.lumina.pagos.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

/**
 * Cuerpo de POST /api/pagos/simular (pago simulado desde el carrito).
 *
 * @param ordenId identificador de la orden
 * @param email   correo del usuario que paga
 * @param monto   monto pagado
 */
public record SimularPagoRequest(
        @NotBlank(message = "El ordenId es obligatorio")
        String ordenId,

        @NotBlank(message = "El email es obligatorio")
        String email,

        @NotNull(message = "El monto es obligatorio")
        @Positive(message = "El monto debe ser mayor que cero")
        BigDecimal monto
) {
}

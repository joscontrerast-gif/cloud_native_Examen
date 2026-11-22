package com.lumina.bff.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

/**
 * Cuerpo de POST /api/admin/topics.
 *
 * @param name              nombre del topic (letras, números, '.', '_' y '-')
 * @param partitions        cantidad de particiones (1 a 50)
 * @param replicationFactor factor de replicación (1 a 3)
 */
public record CrearTopicRequest(
        @NotBlank(message = "El nombre del topic es obligatorio")
        @Pattern(regexp = "^[a-zA-Z0-9._-]{1,249}$",
                message = "Use solo letras, números, '.', '_' o '-' (máximo 249 caracteres)")
        String name,

        @NotNull(message = "Las particiones son obligatorias")
        @Min(value = 1, message = "Debe haber al menos 1 partición")
        @Max(value = 50, message = "El máximo es 50 particiones")
        Integer partitions,

        @NotNull(message = "El factor de replicación es obligatorio")
        @Min(value = 1, message = "El factor de replicación mínimo es 1")
        @Max(value = 3, message = "El factor de replicación máximo es 3")
        Short replicationFactor
) {
}

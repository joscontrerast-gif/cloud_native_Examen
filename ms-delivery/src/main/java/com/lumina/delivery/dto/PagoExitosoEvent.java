package com.lumina.delivery.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/** Evento "pago exitoso" publicado por ms-pagos en Kafka (mismo contrato JSON). */
public class PagoExitosoEvent {

    private String ordenId;
    private String usuarioEmail;
    private BigDecimal monto;
    private LocalDateTime fechaPago;

    public PagoExitosoEvent() {}

    public String getOrdenId() { return ordenId; }
    public void setOrdenId(String ordenId) { this.ordenId = ordenId; }

    public String getUsuarioEmail() { return usuarioEmail; }
    public void setUsuarioEmail(String usuarioEmail) { this.usuarioEmail = usuarioEmail; }

    public BigDecimal getMonto() { return monto; }
    public void setMonto(BigDecimal monto) { this.monto = monto; }

    public LocalDateTime getFechaPago() { return fechaPago; }
    public void setFechaPago(LocalDateTime fechaPago) { this.fechaPago = fechaPago; }
}

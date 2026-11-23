package com.lumina.bodega.service;

import com.lumina.bodega.dto.AlertaStockDto;
import com.lumina.bodega.dto.ItemPedidoDto;
import com.lumina.bodega.entity.AlertaStock;
import com.lumina.bodega.entity.Inventario;
import com.lumina.bodega.messaging.AlertaStockKafkaProducer;
import com.lumina.bodega.repository.AlertaStockRepository;
import com.lumina.bodega.repository.InventarioRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class BodegaService {

    private final InventarioRepository inventarioRepository;
    private final AlertaStockRepository alertaStockRepository;

    @Autowired(required = false)
    private AlertaStockKafkaProducer alertaStockKafkaProducer;

    public List<Inventario> listarInventario() {
        return inventarioRepository.findAll();
    }

    @Transactional
    public Inventario crearInventario(Inventario inventario) {
        return inventarioRepository.save(inventario);
    }

    public Inventario obtenerStock(Long productoId) {
        return inventarioRepository.findByProductoId(productoId)
                .orElseThrow(() -> new RuntimeException("No hay registro de inventario para producto: " + productoId));
    }

    @Transactional
    public Inventario actualizarStock(Long productoId, Integer cantidad) {
        Inventario inventario = obtenerStock(productoId);
        inventario.setStock(inventario.getStock() + cantidad);

        if (inventario.isStockCritico()) {
            generarAlerta(inventario);
        } else {
            resolverAlertas(inventario.getProductoId());
        }

        return inventarioRepository.save(inventario);
    }

    @Transactional
    public Inventario descontarStock(Long productoId, Integer cantidad) {
        Inventario inventario = obtenerStock(productoId);

        if (inventario.getStock() < cantidad) {
            throw new RuntimeException("Stock insuficiente para producto: " + productoId);
        }

        inventario.setStock(inventario.getStock() - cantidad);

        if (inventario.isStockCritico()) {
            generarAlerta(inventario);
        } else {
            resolverAlertas(inventario.getProductoId());
        }

        return inventarioRepository.save(inventario);
    }

    public List<AlertaStock> obtenerAlertas() {
        return alertaStockRepository.findByResueltaFalseOrderByFechaCreacionDesc();
    }

    @Transactional
    public void descontarStockPedido(List<ItemPedidoDto> items) {
        if (items == null || items.isEmpty()) {
            throw new IllegalArgumentException("El pedido no contiene items para descontar");
        }

        for (ItemPedidoDto item : items) {
            descontarStock(item.getProductoId(), item.getCantidad());
        }
    }

    private void generarAlerta(Inventario inventario) {
        List<AlertaStock> existentes = alertaStockRepository.findByProductoIdAndResueltaFalse(inventario.getProductoId());
        AlertaStock alertaPersistida;

        if (!existentes.isEmpty()) {
            AlertaStock primera = existentes.get(0);
            primera.setStockActual(inventario.getStock());
            alertaPersistida = alertaStockRepository.save(primera);
            for (int i = 1; i < existentes.size(); i++) {
                existentes.get(i).setResuelta(true);
                alertaStockRepository.save(existentes.get(i));
            }
        } else {
            AlertaStock alerta = AlertaStock.builder()
                    .productoId(inventario.getProductoId())
                    .nombreProducto(inventario.getNombreProducto())
                    .stockActual(inventario.getStock())
                    .stockMinimo(inventario.getStockMinimo())
                    .build();
            alertaPersistida = alertaStockRepository.save(alerta);
            log.warn("ALERTA: Stock critico para producto {} - Stock actual: {}, Minimo: {}",
                    inventario.getNombreProducto(), inventario.getStock(), inventario.getStockMinimo());
        }

        if (alertaStockKafkaProducer != null && alertaPersistida != null) {
            AlertaStockDto dto = AlertaStockDto.builder()
                    .id(alertaPersistida.getId())
                    .productoId(alertaPersistida.getProductoId())
                    .nombreProducto(alertaPersistida.getNombreProducto())
                    .stockActual(alertaPersistida.getStockActual())
                    .stockMinimo(alertaPersistida.getStockMinimo())
                    .resuelta(alertaPersistida.getResuelta())
                    .fechaCreacion(alertaPersistida.getFechaCreacion())
                    .build();
            alertaStockKafkaProducer.publicarAlertaStock(dto);
        }
    }

    private void resolverAlertas(Long productoId) {
        List<AlertaStock> alertas = alertaStockRepository.findByProductoIdAndResueltaFalse(productoId);
        for (AlertaStock alerta : alertas) {
            alerta.setResuelta(true);
            alertaStockRepository.save(alerta);
        }
        if (!alertas.isEmpty()) {
            log.info("ALERTA RESUELTA: Stock normalizado para producto {} ({} alertas resueltas)", productoId, alertas.size());
        }
    }
}

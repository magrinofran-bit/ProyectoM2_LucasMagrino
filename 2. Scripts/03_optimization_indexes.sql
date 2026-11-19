-- FleetLogix - Avance 2
-- Indices de optimizacion SQL

-- Indice 1: Viajes completados
CREATE INDEX idx_trips_composite_joins
ON trips(vehicle_id, driver_id, route_id, departure_datetime)
WHERE status = 'completed';

-- Indice 2: Entregas programadas
CREATE INDEX idx_deliveries_scheduled_datetime
ON deliveries(scheduled_datetime, delivery_status)
WHERE delivery_status = 'delivered';

-- Indice 3: Costos de mantenimiento
CREATE INDEX idx_maintenance_vehicle_cost
ON maintenance(vehicle_id, cost);

-- Indice 4: Licencias de conductores activos
CREATE INDEX idx_drivers_status_license
ON drivers(status, license_expiry)
WHERE status = 'active';

-- Indice 5: Informacion de rutas
CREATE INDEX idx_routes_metrics
ON routes(route_id, distance_km, destination_city);
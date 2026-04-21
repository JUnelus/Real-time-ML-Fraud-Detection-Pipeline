-- sql/init.sql
-- Database schema for the upgraded fraud detection system

CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) UNIQUE NOT NULL,
    customer_id VARCHAR(20) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    merchant VARCHAR(100) NOT NULL,
    location VARCHAR(100) NOT NULL,
    transaction_timestamp TIMESTAMP NOT NULL,
    actual_fraud BOOLEAN,
    predicted_fraud BOOLEAN,
    fraud_probability DECIMAL(5,4),
    confidence VARCHAR(20),
    risk_level VARCHAR(20) DEFAULT 'LOW',
    risk_factors JSONB DEFAULT '[]',
    processing_latency_ms INTEGER DEFAULT 0,
    pipeline_status VARCHAR(30) DEFAULT 'processed',
    failure_reason TEXT,
    retry_count INTEGER DEFAULT 0,
    raw_payload JSONB,
    model_version VARCHAR(50),
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_events (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50),
    event_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    stage VARCHAR(30),
    details JSONB DEFAULT '{}',
    raw_payload JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transaction_timestamp ON transactions(transaction_timestamp);
CREATE INDEX IF NOT EXISTS idx_processing_timestamp ON transactions(processing_timestamp);
CREATE INDEX IF NOT EXISTS idx_customer_id ON transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_predicted_fraud ON transactions(predicted_fraud);
CREATE INDEX IF NOT EXISTS idx_amount ON transactions(amount);
CREATE INDEX IF NOT EXISTS idx_transactions_pipeline_status ON transactions(pipeline_status);
CREATE INDEX IF NOT EXISTS idx_transactions_risk_level ON transactions(risk_level);
CREATE INDEX IF NOT EXISTS idx_pipeline_events_status ON pipeline_events(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_events_created_at ON pipeline_events(created_at DESC);

CREATE OR REPLACE VIEW fraud_stats AS
SELECT
    DATE(processing_timestamp) AS date,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN predicted_fraud = true THEN 1 ELSE 0 END) AS fraud_detected,
    ROUND(AVG(CASE WHEN predicted_fraud = true THEN 1.0 ELSE 0.0 END) * 100, 2) AS fraud_rate_percent,
    ROUND(AVG(amount), 2) AS avg_amount,
    SUM(CASE WHEN predicted_fraud = true THEN amount ELSE 0 END) AS total_fraud_amount,
    ROUND(AVG(processing_latency_ms), 2) AS avg_latency_ms,
    SUM(CASE WHEN retry_count > 0 THEN 1 ELSE 0 END) AS retried_transactions
FROM transactions
GROUP BY DATE(processing_timestamp)
ORDER BY date DESC;

CREATE OR REPLACE VIEW hourly_stats AS
SELECT
    DATE_TRUNC('hour', processing_timestamp) AS hour,
    COUNT(*) AS transaction_count,
    SUM(CASE WHEN predicted_fraud = true THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(AVG(fraud_probability), 4) AS avg_fraud_probability,
    ROUND(AVG(processing_latency_ms), 2) AS avg_latency_ms
FROM transactions
WHERE processing_timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', processing_timestamp)
ORDER BY hour DESC;

INSERT INTO transactions (
    transaction_id, customer_id, amount, merchant, location,
    transaction_timestamp, actual_fraud, predicted_fraud,
    fraud_probability, confidence, risk_level, risk_factors,
    processing_latency_ms, pipeline_status, retry_count, model_version
) VALUES
('SAMPLE_001', 'CUST_1001', 1500.00, 'Luxury Store', 'International',
 NOW() - INTERVAL '1 hour', true, true, 0.95, 'High', 'HIGH', '["high_amount", "international_location", "luxury_merchant"]', 42, 'processed', 0, 'random_forest_v2'),
('SAMPLE_002', 'CUST_1002', 45.99, 'Amazon', 'New York, NY',
 NOW() - INTERVAL '30 minutes', false, false, 0.05, 'High', 'LOW', '["routine_behavior"]', 18, 'processed', 0, 'random_forest_v2'),
('SAMPLE_003', 'CUST_1003', 875.50, 'Electronics Store', 'Los Angeles, CA',
 NOW() - INTERVAL '15 minutes', true, true, 0.88, 'High', 'HIGH', '["elevated_amount"]', 25, 'processed', 1, 'random_forest_v2')
ON CONFLICT (transaction_id) DO NOTHING;

INSERT INTO pipeline_events (transaction_id, event_type, status, stage, details)
VALUES
('SAMPLE_001', 'transaction_processed', 'success', 'database', '{"save_status": "inserted", "risk_level": "HIGH"}'),
('SAMPLE_001', 'high_risk_alert', 'success', 'detection', '{"fraud_probability": 0.95}'),
('SAMPLE_003', 'processing_retry', 'error', 'processing', '{"attempt": 1, "reason": "temporary database timeout"}')
ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION get_recent_fraud_alerts(hours_back INTEGER DEFAULT 1)
RETURNS TABLE (
    transaction_id VARCHAR(50),
    amount DECIMAL(10,2),
    merchant VARCHAR(100),
    location VARCHAR(100),
    fraud_probability DECIMAL(5,4),
    risk_level VARCHAR(20),
    minutes_ago INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        t.transaction_id,
        t.amount,
        t.merchant,
        t.location,
        t.fraud_probability,
        t.risk_level,
        (EXTRACT(EPOCH FROM (NOW() - t.processing_timestamp))::INTEGER / 60)
    FROM transactions t
    WHERE t.predicted_fraud = true
      AND t.processing_timestamp >= NOW() - (hours_back || ' hours')::INTERVAL
    ORDER BY t.processing_timestamp DESC;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fraud_user') THEN
        EXECUTE 'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO fraud_user';
        EXECUTE 'GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO fraud_user';
        EXECUTE 'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO fraud_user';
    END IF;
END $$;

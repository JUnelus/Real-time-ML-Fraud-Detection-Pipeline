-- sql/init.sql
-- Database schema for fraud detection system

-- Create main transactions table
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
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_transaction_timestamp ON transactions(transaction_timestamp);
CREATE INDEX IF NOT EXISTS idx_processing_timestamp ON transactions(processing_timestamp);
CREATE INDEX IF NOT EXISTS idx_customer_id ON transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_predicted_fraud ON transactions(predicted_fraud);
CREATE INDEX IF NOT EXISTS idx_amount ON transactions(amount);

-- Create fraud statistics view
CREATE OR REPLACE VIEW fraud_stats AS
SELECT
    DATE(processing_timestamp) as date,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN predicted_fraud = true THEN 1 ELSE 0 END) as fraud_detected,
    ROUND(AVG(CASE WHEN predicted_fraud = true THEN 1.0 ELSE 0.0 END) * 100, 2) as fraud_rate_percent,
    ROUND(AVG(amount), 2) as avg_amount,
    SUM(CASE WHEN predicted_fraud = true THEN amount ELSE 0 END) as total_fraud_amount
FROM transactions
GROUP BY DATE(processing_timestamp)
ORDER BY date DESC;

-- Create hourly statistics view
CREATE OR REPLACE VIEW hourly_stats AS
SELECT
    DATE_TRUNC('hour', processing_timestamp) as hour,
    COUNT(*) as transaction_count,
    SUM(CASE WHEN predicted_fraud = true THEN 1 ELSE 0 END) as fraud_count,
    ROUND(AVG(fraud_probability), 4) as avg_fraud_probability
FROM transactions
WHERE processing_timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', processing_timestamp)
ORDER BY hour DESC;

-- Insert some sample data for testing
INSERT INTO transactions (
    transaction_id, customer_id, amount, merchant, location,
    transaction_timestamp, actual_fraud, predicted_fraud,
    fraud_probability, confidence
) VALUES
('SAMPLE_001', 'CUST_1001', 1500.00, 'Luxury Store', 'International',
 NOW() - INTERVAL '1 hour', true, true, 0.95, 'High'),
('SAMPLE_002', 'CUST_1002', 45.99, 'Amazon', 'New York, NY',
 NOW() - INTERVAL '30 minutes', false, false, 0.05, 'High'),
('SAMPLE_003', 'CUST_1003', 875.50, 'Electronics Store', 'Los Angeles, CA',
 NOW() - INTERVAL '15 minutes', true, true, 0.88, 'High')
ON CONFLICT (transaction_id) DO NOTHING;

-- Create function to get recent fraud alerts
CREATE OR REPLACE FUNCTION get_recent_fraud_alerts(hours_back INTEGER DEFAULT 1)
RETURNS TABLE (
    transaction_id VARCHAR(50),
    amount DECIMAL(10,2),
    merchant VARCHAR(100),
    location VARCHAR(100),
    fraud_probability DECIMAL(5,4),
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
        EXTRACT(EPOCH FROM (NOW() - t.processing_timestamp))::INTEGER / 60 as minutes_ago
    FROM transactions t
    WHERE t.predicted_fraud = true
      AND t.processing_timestamp >= NOW() - (hours_back || ' hours')::INTERVAL
    ORDER BY t.processing_timestamp DESC;
END;
$$ LANGUAGE plpgsql;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO fraud_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO fraud_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO fraud_user;
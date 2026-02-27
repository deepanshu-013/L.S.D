-- L.S.D Authentication System Database Schema
-- PostgreSQL 15+

-- Enable UUID extension if not already enabled
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ═══════════════════════════════════════════════════════════
-- USERS TABLE
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    username VARCHAR(30) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    role VARCHAR(20) DEFAULT 'user' CHECK (role IN ('user', 'admin', 'service')),
    
    -- Email verification
    email_verified BOOLEAN DEFAULT false,
    verification_token VARCHAR(255),
    verification_expires_at TIMESTAMP,
    
    -- Password reset
    reset_token VARCHAR(255),
    reset_expires_at TIMESTAMP,
    
    -- Metadata
    api_key VARCHAR(255),  -- Legacy API key (optional)
    last_login_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for users
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_username ON users(username);
CREATE INDEX idx_users_verification_token ON users(verification_token) WHERE verification_token IS NOT NULL;
CREATE INDEX idx_users_reset_token ON users(reset_token) WHERE reset_token IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- SESSIONS TABLE
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS sessions (
    id VARCHAR(32) PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    refresh_token_hash VARCHAR(255) NOT NULL,
    
    -- Device information
    user_agent TEXT,
    ip_address VARCHAR(45),  -- IPv6 compatible
    device_name VARCHAR(255),
    
    -- Session state
    revoked BOOLEAN DEFAULT false,
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT valid_expiry CHECK (expires_at > created_at)
);

-- Indexes for sessions
CREATE INDEX idx_sessions_user_id ON sessions(user_id);
CREATE INDEX idx_sessions_refresh_token ON sessions(refresh_token_hash);
CREATE INDEX idx_sessions_expires ON sessions(expires_at) WHERE NOT revoked;
CREATE INDEX idx_sessions_active ON sessions(user_id, expires_at) WHERE NOT revoked;

-- ═══════════════════════════════════════════════════════════
-- API KEYS TABLE
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS api_keys (
    id VARCHAR(32) PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    
    -- Key identification
    key_hash VARCHAR(255) UNIQUE NOT NULL,
    key_prefix VARCHAR(16) NOT NULL,  -- For identification (lsd_live_xxxx...)
    
    -- Key metadata
    name VARCHAR(100) NOT NULL,
    description TEXT,
    
    -- Permissions
    scopes JSONB DEFAULT '["read"]'::jsonb,
    rate_limit INTEGER DEFAULT 1000,  -- requests per minute
    
    -- Key state
    revoked BOOLEAN DEFAULT false,
    last_used_at TIMESTAMP,
    expires_at TIMESTAMP,  -- Optional expiration
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT valid_rate_limit CHECK (rate_limit > 0 AND rate_limit <= 50000)
);

-- Indexes for API keys
CREATE INDEX idx_api_keys_user_id ON api_keys(user_id);
CREATE INDEX idx_api_keys_hash ON api_keys(key_hash) WHERE NOT revoked;
CREATE INDEX idx_api_keys_prefix ON api_keys(key_prefix);

-- ═══════════════════════════════════════════════════════════
-- AUDIT LOG TABLE (Optional but Recommended)
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS audit_logs (
    id SERIAL PRIMARY KEY,
    
    -- Actor
    user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    api_key_id VARCHAR(32) REFERENCES api_keys(id) ON DELETE SET NULL,
    
    -- Action details
    action VARCHAR(50) NOT NULL,  -- login, logout, api_key_create, etc.
    resource VARCHAR(100),
    resource_id VARCHAR(255),
    
    -- Request context
    ip_address VARCHAR(45),
    user_agent TEXT,
    
    -- Result
    success BOOLEAN DEFAULT true,
    error_message TEXT,
    
    -- Additional data
    metadata JSONB,
    
    -- Timestamp
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for audit logs
CREATE INDEX idx_audit_logs_user ON audit_logs(user_id, created_at DESC);
CREATE INDEX idx_audit_logs_action ON audit_logs(action, created_at DESC);
CREATE INDEX idx_audit_logs_created ON audit_logs(created_at DESC);

-- ═══════════════════════════════════════════════════════════
-- RATE LIMIT ENTRIES TABLE (Alternative to Redis)
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rate_limit_entries (
    id SERIAL PRIMARY KEY,
    identifier VARCHAR(255) NOT NULL,  -- IP, user_id, or api_key_id
    endpoint VARCHAR(100),
    window_start TIMESTAMP NOT NULL,
    request_count INTEGER DEFAULT 1,
    
    -- Constraints
    CONSTRAINT unique_rate_limit UNIQUE (identifier, endpoint, window_start)
);

-- Index for rate limiting lookups
CREATE INDEX idx_rate_limit_lookup ON rate_limit_entries(identifier, endpoint, window_start);

-- Auto-cleanup old rate limit entries (run periodically)
-- DELETE FROM rate_limit_entries WHERE window_start < NOW() - INTERVAL '1 hour';

-- ═══════════════════════════════════════════════════════════
-- HELPER FUNCTIONS
-- ═══════════════════════════════════════════════════════════

-- Update timestamp trigger
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply trigger to tables with updated_at
CREATE TRIGGER update_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER update_api_keys_updated_at
    BEFORE UPDATE ON api_keys
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

-- ═══════════════════════════════════════════════════════════
-- CLEANUP FUNCTIONS (Run via cron or pg_cron)
-- ═══════════════════════════════════════════════════════════

-- Clean up expired sessions
CREATE OR REPLACE FUNCTION cleanup_expired_sessions()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM sessions 
    WHERE expires_at < NOW() OR revoked = true;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Clean up old rate limit entries
CREATE OR REPLACE FUNCTION cleanup_rate_limits()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM rate_limit_entries 
    WHERE window_start < NOW() - INTERVAL '1 hour';
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Clean up old audit logs (keep 90 days)
CREATE OR REPLACE FUNCTION cleanup_old_audit_logs()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM audit_logs 
    WHERE created_at < NOW() - INTERVAL '90 days';
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- ═══════════════════════════════════════════════════════════
-- INITIAL DATA (Optional)
-- ═══════════════════════════════════════════════════════════

-- Insert admin user (change password after first login!)
-- Password: Admin@2026 (bcrypt hash below)
-- INSERT INTO users (email, username, password_hash, name, role, email_verified)
-- VALUES (
--     'admin@lsd.local',
--     'admin',
--     '$2a$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/X4.flLWrYLYfCDVGS',
--     'System Administrator',
--     'admin',
--     true
-- );

-- ═══════════════════════════════════════════════════════════
-- GRANTS (Adjust based on your security requirements)
-- ═══════════════════════════════════════════════════════════

-- Example: Create application user
-- CREATE USER lsd_app WITH PASSWORD 'secure_password';
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO lsd_app;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO lsd_app;

COMMIT;

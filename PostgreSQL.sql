-- ==============================================================================
-- 1. Custom Enum Types (មាន Safe Check ការពារ Duplicate Type)
-- ==============================================================================
DO $$ BEGIN
    CREATE TYPE currency_type AS ENUM ('USD', 'KHR');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE device_status AS ENUM ('ACTIVE', 'INACTIVE', 'MAINTENANCE');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE tx_status AS ENUM ('PENDING', 'PROCESSED', 'FAILED', 'DUPLICATE');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- ==============================================================================
-- 2. Merchants Table (ព័ត៌មានម្ចាស់ហាង/អាជីវករ)
-- ==============================================================================
CREATE TABLE IF NOT EXISTS merchants (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    owner_phone VARCHAR(20) NOT NULL UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ==============================================================================
-- 3. Devices Table (គ្រប់គ្រងឧបករណ៍ Soundbox Y6B)
-- ==============================================================================
CREATE TABLE IF NOT EXISTS devices (
    id SERIAL PRIMARY KEY,
    merchant_id INT REFERENCES merchants(id) ON DELETE SET NULL,
    device_sn VARCHAR(100) NOT NULL UNIQUE,
    device_model VARCHAR(50) DEFAULT 'Y6B',
    telegram_chat_id VARCHAR(100) UNIQUE,
    status device_status DEFAULT 'ACTIVE',
    last_heartbeat TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ==============================================================================
-- 4. Transactions Table (កត់ត្រាការទូទាត់ & ការពារ Duplicate តាម Unique Bank Tx)
-- ==============================================================================
CREATE TABLE IF NOT EXISTS transactions (
    id BIGSERIAL PRIMARY KEY,
    device_id INT NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    bank_name VARCHAR(50) NOT NULL,
    bank_tx_id VARCHAR(150) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    currency currency_type NOT NULL DEFAULT 'USD',
    payer_name VARCHAR(255),
    raw_telegram_message TEXT,
    status tx_status DEFAULT 'PROCESSED',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_bank_tx UNIQUE (bank_name, bank_tx_id)
);

-- ==============================================================================
-- 5. Group Users Table (សម្រាប់ Anti-Fraud & សមាជិកក្នុង Telegram Group)
-- ==============================================================================
CREATE TABLE IF NOT EXISTS group_users (
    id SERIAL PRIMARY KEY,
    chat_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    username VARCHAR(100),
    full_name VARCHAR(150),
    is_authorized BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_chat_user UNIQUE (chat_id, user_id)
);

-- ==============================================================================
-- 6. Official Bank Bots Table (បញ្ជី Bot ផ្លូវការរបស់ធនាគារ)
-- ==============================================================================
CREATE TABLE IF NOT EXISTS official_bank_bots (
    id SERIAL PRIMARY KEY,
    bank_name VARCHAR(50) NOT NULL,
    bot_user_id VARCHAR(50) NOT NULL UNIQUE,
    bot_username VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- បញ្ចូល Bot IDs របស់ធនាគារគំរូ
INSERT INTO official_bank_bots (bank_name, bot_user_id, bot_username, is_active) 
VALUES 
    ('ABA Bank Bot', '285389897', 'ababank_bot', TRUE),
    ('ACLEDA Bank Bot', '987654321', 'acleda_bot', TRUE)
ON CONFLICT (bot_user_id) DO UPDATE 
SET bank_name = EXCLUDED.bank_name,
    bot_username = EXCLUDED.bot_username,
    is_active = TRUE;

-- ==============================================================================
-- 7. High-Performance Indexes (បង្កើនល្បឿន Query ឱ្យនៅក្រោម 5ms)
-- ==============================================================================
CREATE INDEX IF NOT EXISTS idx_devices_telegram_chat_id ON devices(telegram_chat_id);
CREATE INDEX IF NOT EXISTS idx_devices_sn ON devices(device_sn);
CREATE INDEX IF NOT EXISTS idx_transactions_bank_tx ON transactions(bank_name, bank_tx_id);
CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_group_users_lookup ON group_users(chat_id, user_id);
CREATE INDEX IF NOT EXISTS idx_official_bank_bots_active ON official_bank_bots(bot_user_id) WHERE is_active = TRUE;
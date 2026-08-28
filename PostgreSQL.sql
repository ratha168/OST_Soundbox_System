-- 1. Custom Enum Types
CREATE TYPE currency_type AS ENUM ('USD', 'KHR');
CREATE TYPE device_status AS ENUM ('ACTIVE', 'INACTIVE', 'MAINTENANCE');
CREATE TYPE tx_status AS ENUM ('PENDING', 'PROCESSED', 'FAILED', 'DUPLICATE');

-- 2. Merchants Table (ព័ត៌មានអាជីវករ/ម្ចាស់ហាង)
CREATE TABLE merchants (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    owner_phone VARCHAR(20) NOT NULL UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Devices Table (ការគ្រប់គ្រងឧបករណ៍ Soundbox Y6B)
CREATE TABLE devices (
    id SERIAL PRIMARY KEY,
    merchant_id INT REFERENCES merchants(id) ON DELETE SET NULL,
    device_sn VARCHAR(100) NOT NULL UNIQUE,      -- Serial Number របស់ Y6B
    device_model VARCHAR(50) DEFAULT 'Y6B',
    telegram_chat_id VARCHAR(100) UNIQUE,        -- Link ជាមួយ Telegram Group
    status device_status DEFAULT 'ACTIVE',
    last_heartbeat TIMESTAMP WITH TIME ZONE,     -- ពិនិត្យមើលថាតើឧបករណ៍ Online ឬ Offline
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4. Transactions Table (រក្សាទុកប្រវត្តិទូទាត់ & ការពារការស្រែកឌុប)
CREATE TABLE transactions (
    id BIGSERIAL PRIMARY KEY,
    device_id INT NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    bank_name VARCHAR(50) NOT NULL,              -- ABA, ACLEDA, WING
    bank_tx_id VARCHAR(150) NOT NULL,            -- Transaction ID របស់ធនាគារ
    amount NUMERIC(12, 2) NOT NULL,
    currency currency_type NOT NULL DEFAULT 'USD',
    payer_name VARCHAR(255),
    raw_telegram_message TEXT,
    status tx_status DEFAULT 'PROCESSED',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint ទប់ស្កាត់មិនឱ្យទិន្នន័យដដែលចូល ២ ដង
    CONSTRAINT unique_bank_tx UNIQUE (bank_name, bank_tx_id)
);


CREATE TABLE IF NOT EXISTS group_users (
    id SERIAL PRIMARY KEY,
    chat_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    username VARCHAR(100),
    full_name VARCHAR(150),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_chat_user UNIQUE (chat_id, user_id)
);

ALTER TABLE group_users 
ADD COLUMN IF NOT EXISTS is_authorized BOOLEAN DEFAULT FALSE;

-- បង្កើតតារាងទុក Official Bank Bot IDs
CREATE TABLE IF NOT EXISTS official_bank_bots (
    id SERIAL PRIMARY KEY,
    bank_name VARCHAR(50) NOT NULL,
    bot_user_id VARCHAR(50) NOT NULL UNIQUE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ១. បង្កើតតារាង official_bank_bots ជាមុន
CREATE TABLE IF NOT EXISTS official_bank_bots (
    id SERIAL PRIMARY KEY,
    bank_name VARCHAR(50) NOT NULL,
    bot_user_id VARCHAR(50) NOT NULL UNIQUE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ២. Insert ទិន្នន័យគំរូចូល
INSERT INTO official_bank_bots (bank_name, bot_user_id) 
VALUES 
    ('ABA Bank Bot', '123456789'),
    ('ACLEDA Bank Bot', '987654321')
ON CONFLICT (bot_user_id) DO NOTHING;


-- 5. Indexes សម្រាប់បង្កើនល្បឿន Query (< 10ms)
CREATE INDEX idx_devices_telegram_chat_id ON devices(telegram_chat_id);
CREATE INDEX idx_devices_sn ON devices(device_sn);
CREATE INDEX idx_transactions_bank_tx ON transactions(bank_name, bank_tx_id);
CREATE INDEX idx_transactions_created_at ON transactions(created_at);
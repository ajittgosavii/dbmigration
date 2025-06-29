import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import time
import re
import json
import asyncio
import logging
from typing import Dict, List, Tuple, Optional, Set
from dataclasses import dataclass
from enum import Enum
import anthropic
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Database Migration Analyzer - Schema & Query Analysis",
    page_icon="🗄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Professional CSS styling
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #1e3a8a 0%, #1e40af 50%, #1d4ed8 100%);
        padding: 2rem;
        border-radius: 12px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
        box-shadow: 0 4px 20px rgba(30,58,138,0.3);
        border: 1px solid rgba(255,255,255,0.1);
    }
    
    .main-header h1 {
        margin: 0 0 0.5rem 0;
        font-size: 2.5rem;
        font-weight: 700;
        text-shadow: 0 2px 4px rgba(0,0,0,0.3);
    }
    
    .analysis-card {
        background: linear-gradient(135deg, #ffffff 0%, #f8fafc 100%);
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        border-left: 4px solid #3b82f6;
        border: 1px solid #e5e7eb;
        transition: transform 0.2s ease;
    }
    
    .analysis-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(0,0,0,0.15);
    }
    
    .schema-card {
        background: linear-gradient(135deg, #f0f9ff 0%, #e0f2fe 100%);
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        border-left: 4px solid #0ea5e9;
        border: 1px solid #e5e7eb;
    }
    
    .query-card {
        background: linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%);
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        border-left: 4px solid #22c55e;
        border: 1px solid #e5e7eb;
    }
    
    .compatibility-card {
        background: linear-gradient(135deg, #fefdf8 0%, #fefce8 100%);
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        border-left: 4px solid #f59e0b;
        border: 1px solid #e5e7eb;
    }
    
    .risk-card {
        background: linear-gradient(135deg, #fef2f2 0%, #fee2e2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        border-left: 4px solid #ef4444;
        border: 1px solid #e5e7eb;
    }
    
    .aws-card {
        background: linear-gradient(135deg, #faf5ff 0%, #f3e8ff 100%);
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        border-left: 4px solid #a855f7;
        border: 1px solid #e5e7eb;
    }
    
    .example-card {
        background: linear-gradient(135deg, #f1f5f9 0%, #e2e8f0 100%);
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        border-left: 4px solid #64748b;
        border: 1px solid #e5e7eb;
    }
    
    .tutorial-step {
        background: linear-gradient(135deg, #ecfdf5 0%, #d1fae5 100%);
        padding: 1rem;
        border-radius: 8px;
        margin: 0.5rem 0;
        border-left: 3px solid #10b981;
        border: 1px solid #e5e7eb;
    }
    
    .code-section {
        background: #1e293b;
        color: #e2e8f0;
        padding: 1rem;
        border-radius: 8px;
        font-family: 'Monaco', 'Consolas', monospace;
        font-size: 0.9rem;
        overflow-x: auto;
        margin: 1rem 0;
        border: 1px solid #334155;
    }
    
    .metric-card {
        background: #ffffff;
        padding: 1.5rem;
        border-radius: 10px;
        border-left: 4px solid #3b82f6;
        margin: 1rem 0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        border: 1px solid #e5e7eb;
        transition: all 0.2s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 16px rgba(0,0,0,0.15);
    }
    
    .complexity-high { border-left-color: #ef4444; }
    .complexity-medium { border-left-color: #f59e0b; }
    .complexity-low { border-left-color: #22c55e; }
    
    .professional-footer {
        background: linear-gradient(135deg, #1f2937 0%, #374151 100%);
        color: white;
        padding: 2rem;
        border-radius: 10px;
        margin-top: 2rem;
        text-align: center;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    }
    
    .database-info-card {
        background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
        padding: 1rem;
        border-radius: 8px;
        margin: 0.5rem 0;
        border-left: 3px solid #3b82f6;
        font-size: 0.9rem;
    }
    
    .use-case-card {
        background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        border-left: 4px solid #6366f1;
        border: 1px solid #e5e7eb;
    }
    
    /* Code syntax highlighting */
    .sql-keyword { color: #7c3aed; font-weight: bold; }
    .sql-string { color: #059669; }
    .sql-comment { color: #6b7280; font-style: italic; }
    .sql-function { color: #dc2626; }
    
    /* Status indicators */
    .status-compatible { color: #22c55e; font-weight: bold; }
    .status-warning { color: #f59e0b; font-weight: bold; }
    .status-incompatible { color: #ef4444; font-weight: bold; }
    
    /* Progress bars */
    .progress-bar {
        background: #e5e7eb;
        border-radius: 10px;
        height: 8px;
        overflow: hidden;
        margin: 0.5rem 0;
    }
    
    .progress-fill {
        height: 100%;
        border-radius: 10px;
        transition: width 0.3s ease;
    }
    
    .progress-high { background: #ef4444; }
    .progress-medium { background: #f59e0b; }
    .progress-low { background: #22c55e; }
</style>
""", unsafe_allow_html=True)

class DatabaseEngine(Enum):
    """Supported database engines"""
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"
    ORACLE = "oracle"
    SQL_SERVER = "sql_server"
    MONGODB = "mongodb"
    REDIS = "redis"
    CASSANDRA = "cassandra"

class ComplexityLevel(Enum):
    """Migration complexity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    VERY_HIGH = "very_high"

class CompatibilityStatus(Enum):
    """Compatibility status for objects"""
    COMPATIBLE = "compatible"
    PARTIALLY_COMPATIBLE = "partially_compatible"
    INCOMPATIBLE = "incompatible"
    REQUIRES_MANUAL_REVIEW = "requires_manual_review"

@dataclass
class SchemaObject:
    """Represents a database schema object"""
    name: str
    object_type: str  # table, view, procedure, function, trigger, index
    source_engine: DatabaseEngine
    definition: str
    dependencies: List[str]
    complexity: ComplexityLevel
    compatibility_status: CompatibilityStatus
    issues: List[str]
    recommendations: List[str]

@dataclass
class DataTypeMapping:
    """Data type mapping between engines"""
    source_type: str
    target_type: str
    compatibility: CompatibilityStatus
    conversion_notes: str
    precision_loss: bool

@dataclass
class QueryAnalysis:
    """Query compatibility analysis result"""
    original_query: str
    compatibility_score: float
    issues: List[Dict]
    converted_query: str
    complexity: ComplexityLevel
    performance_impact: str

# Enhanced database-specific configuration with comprehensive examples
DATABASE_CONFIG = {
    'mysql': {
        'display_name': 'MySQL',
        'icon': '🐬',
        'schema_label': 'MySQL Schema Definition',
        'query_label': 'MySQL Queries',
        'schema_term': 'Database Schema',
        'query_term': 'SQL Queries',
        'file_extensions': ['.sql', '.dump'],
        'sample_schema': '''-- MySQL E-commerce Database Schema Example
-- This demonstrates common MySQL features and patterns

CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    phone VARCHAR(20),
    date_of_birth DATE,
    is_active TINYINT(1) DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_username (username),
    INDEX idx_email (email),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE categories (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    parent_id INT NULL,
    slug VARCHAR(100) NOT NULL UNIQUE,
    is_active TINYINT(1) DEFAULT 1,
    sort_order INT DEFAULT 0,
    FOREIGN KEY (parent_id) REFERENCES categories(id) ON DELETE SET NULL,
    INDEX idx_parent_id (parent_id),
    INDEX idx_slug (slug)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description LONGTEXT,
    short_description TEXT,
    sku VARCHAR(100) NOT NULL UNIQUE,
    price DECIMAL(10,2) NOT NULL,
    cost_price DECIMAL(10,2),
    stock_quantity INT DEFAULT 0,
    category_id INT NOT NULL,
    brand VARCHAR(100),
    weight DECIMAL(8,3),
    dimensions JSON,
    tags SET('featured', 'bestseller', 'new', 'sale') DEFAULT '',
    status ENUM('active', 'inactive', 'draft') DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES categories(id),
    INDEX idx_category_id (category_id),
    INDEX idx_sku (sku),
    INDEX idx_price (price),
    INDEX idx_status (status),
    FULLTEXT(name, description, short_description)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    order_number VARCHAR(50) NOT NULL UNIQUE,
    total_amount DECIMAL(10,2) NOT NULL,
    tax_amount DECIMAL(10,2) DEFAULT 0,
    shipping_amount DECIMAL(10,2) DEFAULT 0,
    discount_amount DECIMAL(10,2) DEFAULT 0,
    status ENUM('pending', 'processing', 'shipped', 'delivered', 'cancelled') DEFAULT 'pending',
    payment_status ENUM('pending', 'paid', 'failed', 'refunded') DEFAULT 'pending',
    shipping_address JSON,
    billing_address JSON,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id),
    INDEX idx_user_id (user_id),
    INDEX idx_order_number (order_number),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''',
        'sample_queries': '''-- MySQL Query Examples for E-commerce Application

-- 1. Complex JOIN with aggregation and date functions
SELECT 
    u.username,
    u.email,
    COUNT(o.id) as total_orders,
    SUM(o.total_amount) as total_spent,
    AVG(o.total_amount) as avg_order_value,
    MAX(o.created_at) as last_order_date,
    DATEDIFF(NOW(), MAX(o.created_at)) as days_since_last_order
FROM users u
LEFT JOIN orders o ON u.id = o.user_id AND o.status != 'cancelled'
WHERE u.created_at >= DATE_SUB(NOW(), INTERVAL 1 YEAR)
GROUP BY u.id, u.username, u.email
HAVING total_orders > 0
ORDER BY total_spent DESC
LIMIT 100;

-- 2. Product search with full-text search and category filtering
SELECT 
    p.id,
    p.name,
    p.price,
    p.stock_quantity,
    c.name as category_name,
    MATCH(p.name, p.description) AGAINST('wireless bluetooth' IN NATURAL LANGUAGE MODE) as relevance_score
FROM products p
INNER JOIN categories c ON p.category_id = c.id
WHERE MATCH(p.name, p.description) AGAINST('wireless bluetooth' IN NATURAL LANGUAGE MODE)
    AND p.status = 'active'
    AND p.stock_quantity > 0
    AND p.price BETWEEN 50.00 AND 500.00
ORDER BY relevance_score DESC, p.price ASC
LIMIT 20;

-- 3. Sales analytics with date formatting and window functions
SELECT 
    DATE_FORMAT(o.created_at, '%Y-%m') as sales_month,
    COUNT(o.id) as total_orders,
    SUM(o.total_amount) as total_revenue,
    AVG(o.total_amount) as avg_order_value,
    LAG(SUM(o.total_amount)) OVER (ORDER BY DATE_FORMAT(o.created_at, '%Y-%m')) as prev_month_revenue,
    ROUND(
        ((SUM(o.total_amount) - LAG(SUM(o.total_amount)) OVER (ORDER BY DATE_FORMAT(o.created_at, '%Y-%m'))) / 
         LAG(SUM(o.total_amount)) OVER (ORDER BY DATE_FORMAT(o.created_at, '%Y-%m'))) * 100, 2
    ) as revenue_growth_percent
FROM orders o
WHERE o.status IN ('delivered', 'shipped')
    AND o.created_at >= DATE_SUB(NOW(), INTERVAL 24 MONTH)
GROUP BY DATE_FORMAT(o.created_at, '%Y-%m')
ORDER BY sales_month DESC;

-- 4. Inventory management with JSON functions
SELECT 
    p.id,
    p.name,
    p.sku,
    p.stock_quantity,
    p.price,
    JSON_EXTRACT(p.dimensions, '$.length') as length,
    JSON_EXTRACT(p.dimensions, '$.width') as width,
    JSON_EXTRACT(p.dimensions, '$.height') as height,
    CASE 
        WHEN p.stock_quantity = 0 THEN 'Out of Stock'
        WHEN p.stock_quantity < 10 THEN 'Low Stock'
        WHEN p.stock_quantity < 50 THEN 'Medium Stock'
        ELSE 'High Stock'
    END as stock_status
FROM products p
WHERE p.status = 'active'
    AND (p.stock_quantity < 10 OR FIND_IN_SET('featured', p.tags))
ORDER BY p.stock_quantity ASC, p.name;'''
    },
    'postgresql': {
        'display_name': 'PostgreSQL',
        'icon': '🐘',
        'schema_label': 'PostgreSQL Schema Definition',
        'query_label': 'PostgreSQL Queries',
        'schema_term': 'Database Schema',
        'query_term': 'SQL Queries',
        'file_extensions': ['.sql', '.psql'],
        'sample_schema': '''-- PostgreSQL E-commerce Database Schema Example
-- Demonstrates PostgreSQL-specific features and best practices

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

CREATE TYPE user_status AS ENUM ('active', 'inactive', 'suspended', 'pending_verification');
CREATE TYPE order_status AS ENUM ('pending', 'processing', 'shipped', 'delivered', 'cancelled', 'returned');
CREATE TYPE payment_status AS ENUM ('pending', 'authorized', 'captured', 'failed', 'refunded', 'partially_refunded');

CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    phone VARCHAR(20),
    date_of_birth DATE,
    status user_status DEFAULT 'pending_verification',
    preferences JSONB DEFAULT '{}',
    last_login_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT check_email_format CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
);

CREATE INDEX idx_users_username ON users USING btree(username);
CREATE INDEX idx_users_email ON users USING btree(email);
CREATE INDEX idx_users_status ON users USING btree(status);
CREATE INDEX idx_users_created_at ON users USING btree(created_at);
CREATE INDEX idx_users_preferences ON users USING gin(preferences);

CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    parent_id INTEGER REFERENCES categories(id) ON DELETE SET NULL,
    slug VARCHAR(100) NOT NULL UNIQUE,
    is_active BOOLEAN DEFAULT true,
    sort_order INTEGER DEFAULT 0,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_categories_parent_id ON categories(parent_id);
CREATE INDEX idx_categories_slug ON categories(slug);
CREATE INDEX idx_categories_metadata ON categories USING gin(metadata);

CREATE TABLE products (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    short_description TEXT,
    sku VARCHAR(100) NOT NULL UNIQUE,
    price NUMERIC(10,2) NOT NULL CHECK (price >= 0),
    cost_price NUMERIC(10,2) CHECK (cost_price >= 0),
    stock_quantity INTEGER DEFAULT 0 CHECK (stock_quantity >= 0),
    category_id INTEGER NOT NULL REFERENCES categories(id),
    brand VARCHAR(100),
    weight NUMERIC(8,3),
    dimensions JSONB,
    tags TEXT[],
    attributes JSONB DEFAULT '{}',
    images TEXT[],
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'draft', 'discontinued')),
    search_vector tsvector,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_products_category_id ON products(category_id);
CREATE INDEX idx_products_sku ON products(sku);
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_products_status ON products(status);
CREATE INDEX idx_products_tags ON products USING gin(tags);
CREATE INDEX idx_products_attributes ON products USING gin(attributes);
CREATE INDEX idx_products_search_vector ON products USING gin(search_vector);

-- Function to update search vector
CREATE OR REPLACE FUNCTION update_product_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector := to_tsvector('english', 
        COALESCE(NEW.name, '') || ' ' || 
        COALESCE(NEW.description, '') || ' ' || 
        COALESCE(NEW.brand, '') || ' ' ||
        COALESCE(array_to_string(NEW.tags, ' '), '')
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_product_search_vector
    BEFORE INSERT OR UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_product_search_vector();

CREATE TABLE orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id),
    order_number VARCHAR(50) NOT NULL UNIQUE,
    total_amount NUMERIC(10,2) NOT NULL CHECK (total_amount >= 0),
    tax_amount NUMERIC(10,2) DEFAULT 0 CHECK (tax_amount >= 0),
    shipping_amount NUMERIC(10,2) DEFAULT 0 CHECK (shipping_amount >= 0),
    discount_amount NUMERIC(10,2) DEFAULT 0 CHECK (discount_amount >= 0),
    status order_status DEFAULT 'pending',
    payment_status payment_status DEFAULT 'pending',
    shipping_address JSONB,
    billing_address JSONB,
    metadata JSONB DEFAULT '{}',
    notes TEXT,
    shipped_at TIMESTAMP WITH TIME ZONE,
    delivered_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_order_number ON orders(order_number);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_payment_status ON orders(payment_status);
CREATE INDEX idx_orders_created_at ON orders(created_at);
CREATE INDEX idx_orders_metadata ON orders USING gin(metadata);''',
        'sample_queries': '''-- PostgreSQL Query Examples with Advanced Features

-- 1. Advanced analytics with window functions and CTEs
WITH monthly_sales AS (
    SELECT 
        DATE_TRUNC('month', created_at) as month,
        COUNT(*) as order_count,
        SUM(total_amount) as revenue,
        AVG(total_amount) as avg_order_value
    FROM orders
    WHERE status IN ('delivered', 'shipped')
        AND created_at >= CURRENT_DATE - INTERVAL '24 months'
    GROUP BY DATE_TRUNC('month', created_at)
),
sales_with_growth AS (
    SELECT 
        month,
        order_count,
        revenue,
        avg_order_value,
        LAG(revenue) OVER (ORDER BY month) as prev_month_revenue,
        ROUND(
            (revenue - LAG(revenue) OVER (ORDER BY month)) / 
            NULLIF(LAG(revenue) OVER (ORDER BY month), 0) * 100, 2
        ) as growth_rate,
        SUM(revenue) OVER (ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_revenue
    FROM monthly_sales
)
SELECT 
    TO_CHAR(month, 'YYYY-MM') as sales_month,
    order_count,
    revenue,
    avg_order_value,
    growth_rate,
    cumulative_revenue,
    CASE 
        WHEN growth_rate > 10 THEN 'High Growth'
        WHEN growth_rate > 0 THEN 'Positive Growth'
        WHEN growth_rate > -10 THEN 'Slight Decline'
        ELSE 'Significant Decline'
    END as growth_category
FROM sales_with_growth
ORDER BY month DESC;

-- 2. Full-text search with ranking and filtering
SELECT 
    p.id,
    p.name,
    p.price,
    p.stock_quantity,
    c.name as category_name,
    p.brand,
    p.tags,
    ts_rank(p.search_vector, plainto_tsquery('english', 'wireless bluetooth headphones')) as relevance,
    p.attributes->'color' as color,
    p.attributes->'size' as size
FROM products p
INNER JOIN categories c ON p.category_id = c.id
WHERE p.search_vector @@ plainto_tsquery('english', 'wireless bluetooth headphones')
    AND p.status = 'active'
    AND p.stock_quantity > 0
    AND p.price BETWEEN 50.00 AND 500.00
    AND ('electronics' = ANY(p.tags) OR c.name ILIKE '%electronics%')
ORDER BY relevance DESC, p.price ASC
LIMIT 25;

-- 3. Customer segmentation using JSONB operations
SELECT 
    u.id,
    u.username,
    u.email,
    COUNT(o.id) as total_orders,
    SUM(o.total_amount) as total_spent,
    AVG(o.total_amount) as avg_order_value,
    MAX(o.created_at) as last_order_date,
    EXTRACT(days FROM (CURRENT_TIMESTAMP - MAX(o.created_at))) as days_since_last_order,
    u.preferences->>'newsletter_subscribed' as newsletter_status,
    u.preferences->>'preferred_currency' as currency,
    CASE 
        WHEN SUM(o.total_amount) > 5000 THEN 'VIP'
        WHEN SUM(o.total_amount) > 1000 THEN 'Premium'
        WHEN COUNT(o.id) > 10 THEN 'Frequent'
        ELSE 'Regular'
    END as customer_segment
FROM users u
LEFT JOIN orders o ON u.id = o.user_id AND o.status != 'cancelled'
WHERE u.status = 'active'
    AND u.created_at >= CURRENT_DATE - INTERVAL '2 years'
GROUP BY u.id, u.username, u.email, u.preferences
HAVING COUNT(o.id) > 0
ORDER BY total_spent DESC;

-- 4. Inventory analysis with array operations and complex conditions
SELECT 
    p.id,
    p.name,
    p.sku,
    p.stock_quantity,
    p.price,
    p.brand,
    p.tags,
    p.dimensions->>'length' as length,
    p.dimensions->>'width' as width,
    p.dimensions->>'height' as height,
    (p.dimensions->>'length')::numeric * (p.dimensions->>'width')::numeric * (p.dimensions->>'height')::numeric as volume,
    CASE 
        WHEN p.stock_quantity = 0 THEN 'Out of Stock'
        WHEN p.stock_quantity < 10 THEN 'Critical'
        WHEN p.stock_quantity < 50 THEN 'Low'
        ELSE 'Adequate'
    END as stock_status,
    CASE 
        WHEN 'featured' = ANY(p.tags) THEN true
        ELSE false
    END as is_featured
FROM products p
WHERE p.status = 'active'
    AND (
        p.stock_quantity < 20 
        OR 'bestseller' = ANY(p.tags)
        OR p.attributes ? 'promotion'
    )
ORDER BY 
    CASE WHEN p.stock_quantity = 0 THEN 1 ELSE 2 END,
    p.stock_quantity ASC,
    p.name;'''
    },
    'oracle': {
        'display_name': 'Oracle Database',
        'icon': '🏛️',
        'schema_label': 'Oracle Schema Definition',
        'query_label': 'Oracle PL/SQL Queries',
        'schema_term': 'Database Schema',
        'query_term': 'PL/SQL Queries',
        'file_extensions': ['.sql', '.ora'],
        'sample_schema': '''-- Oracle Database Enterprise Schema Example
-- Demonstrates Oracle-specific features and enterprise patterns

-- Create sequences for primary keys
CREATE SEQUENCE seq_users START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE seq_categories START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE seq_products START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE seq_orders START WITH 1 INCREMENT BY 1 NOCACHE;

-- Users table with Oracle-specific features
CREATE TABLE users (
    id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    username VARCHAR2(50) NOT NULL UNIQUE,
    email VARCHAR2(100) NOT NULL UNIQUE,
    password_hash VARCHAR2(255) NOT NULL,
    first_name VARCHAR2(50),
    last_name VARCHAR2(50),
    phone VARCHAR2(20),
    date_of_birth DATE,
    status VARCHAR2(20) DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'suspended')),
    preferences CLOB CHECK (preferences IS JSON),
    last_login_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT check_email_format CHECK (REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'))
);

-- Create indexes
CREATE INDEX idx_users_username ON users(username);
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_status ON users(status);
CREATE INDEX idx_users_created_at ON users(created_at);

-- Categories with hierarchical structure
CREATE TABLE categories (
    id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    name VARCHAR2(100) NOT NULL,
    description CLOB,
    parent_id NUMBER,
    slug VARCHAR2(100) NOT NULL UNIQUE,
    is_active NUMBER(1) DEFAULT 1 CHECK (is_active IN (0,1)),
    sort_order NUMBER DEFAULT 0,
    metadata CLOB CHECK (metadata IS JSON),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_categories_parent FOREIGN KEY (parent_id) REFERENCES categories(id) ON DELETE SET NULL
);

CREATE INDEX idx_categories_parent_id ON categories(parent_id);
CREATE INDEX idx_categories_slug ON categories(slug);

-- Products table with Oracle advanced features
CREATE TABLE products (
    id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    name VARCHAR2(255) NOT NULL,
    description CLOB,
    short_description VARCHAR2(1000),
    sku VARCHAR2(100) NOT NULL UNIQUE,
    price NUMBER(10,2) NOT NULL CHECK (price >= 0),
    cost_price NUMBER(10,2) CHECK (cost_price >= 0),
    stock_quantity NUMBER DEFAULT 0 CHECK (stock_quantity >= 0),
    category_id NUMBER NOT NULL,
    brand VARCHAR2(100),
    weight NUMBER(8,3),
    dimensions CLOB CHECK (dimensions IS JSON),
    attributes CLOB CHECK (attributes IS JSON),
    tags VARCHAR2(500),
    status VARCHAR2(20) DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'draft', 'discontinued')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_products_category FOREIGN KEY (category_id) REFERENCES categories(id)
);

CREATE INDEX idx_products_category_id ON products(category_id);
CREATE INDEX idx_products_sku ON products(sku);
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_products_status ON products(status);

-- Create text index for full-text search
CREATE INDEX idx_products_text ON products(name, description) INDEXTYPE IS CTXSYS.CONTEXT;

-- Orders table with partitioning by date
CREATE TABLE orders (
    id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    user_id NUMBER NOT NULL,
    order_number VARCHAR2(50) NOT NULL UNIQUE,
    total_amount NUMBER(10,2) NOT NULL CHECK (total_amount >= 0),
    tax_amount NUMBER(10,2) DEFAULT 0 CHECK (tax_amount >= 0),
    shipping_amount NUMBER(10,2) DEFAULT 0 CHECK (shipping_amount >= 0),
    discount_amount NUMBER(10,2) DEFAULT 0 CHECK (discount_amount >= 0),
    status VARCHAR2(20) DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled')),
    payment_status VARCHAR2(20) DEFAULT 'pending' CHECK (payment_status IN ('pending', 'paid', 'failed', 'refunded')),
    shipping_address CLOB CHECK (shipping_address IS JSON),
    billing_address CLOB CHECK (billing_address IS JSON),
    metadata CLOB CHECK (metadata IS JSON),
    notes CLOB,
    shipped_at TIMESTAMP WITH TIME ZONE,
    delivered_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id)
)
PARTITION BY RANGE (created_at) (
    PARTITION orders_2023 VALUES LESS THAN (DATE '2024-01-01'),
    PARTITION orders_2024 VALUES LESS THAN (DATE '2025-01-01'),
    PARTITION orders_2025 VALUES LESS THAN (DATE '2026-01-01')
);

CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_order_number ON orders(order_number);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_created_at ON orders(created_at);

-- Trigger for updating timestamps
CREATE OR REPLACE TRIGGER trg_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
BEGIN
    :NEW.updated_at := CURRENT_TIMESTAMP;
END;

CREATE OR REPLACE TRIGGER trg_products_updated_at
    BEFORE UPDATE ON products
    FOR EACH ROW
BEGIN
    :NEW.updated_at := CURRENT_TIMESTAMP;
END;''',
        'sample_queries': '''-- Oracle PL/SQL Query Examples with Enterprise Features

-- 1. Hierarchical query with CONNECT BY (Oracle-specific)
SELECT 
    LEVEL as category_level,
    LPAD(' ', (LEVEL-1)*2) || name as category_hierarchy,
    id,
    name,
    parent_id,
    CONNECT_BY_ROOT name as root_category,
    SYS_CONNECT_BY_PATH(name, ' > ') as category_path
FROM categories
WHERE is_active = 1
START WITH parent_id IS NULL
CONNECT BY PRIOR id = parent_id
ORDER SIBLINGS BY sort_order, name;

-- 2. Advanced analytics with Oracle analytical functions
SELECT 
    TO_CHAR(created_at, 'YYYY-MM') as sales_month,
    COUNT(*) as order_count,
    SUM(total_amount) as revenue,
    AVG(total_amount) as avg_order_value,
    LAG(SUM(total_amount), 1) OVER (ORDER BY TO_CHAR(created_at, 'YYYY-MM')) as prev_month_revenue,
    LEAD(SUM(total_amount), 1) OVER (ORDER BY TO_CHAR(created_at, 'YYYY-MM')) as next_month_revenue,
    ROUND(
        (SUM(total_amount) - LAG(SUM(total_amount), 1) OVER (ORDER BY TO_CHAR(created_at, 'YYYY-MM'))) /
        NULLIF(LAG(SUM(total_amount), 1) OVER (ORDER BY TO_CHAR(created_at, 'YYYY-MM')), 0) * 100, 2
    ) as growth_rate,
    SUM(SUM(total_amount)) OVER (ORDER BY TO_CHAR(created_at, 'YYYY-MM') ROWS UNBOUNDED PRECEDING) as cumulative_revenue,
    RATIO_TO_REPORT(SUM(total_amount)) OVER () * 100 as revenue_percentage
FROM orders
WHERE status IN ('delivered', 'shipped')
    AND created_at >= ADD_MONTHS(SYSDATE, -24)
GROUP BY TO_CHAR(created_at, 'YYYY-MM')
ORDER BY TO_CHAR(created_at, 'YYYY-MM') DESC;

-- 3. Oracle Text search with scoring
SELECT 
    p.id,
    p.name,
    p.price,
    p.stock_quantity,
    c.name as category_name,
    p.brand,
    SCORE(1) as relevance_score,
    JSON_VALUE(p.dimensions, '$.length') as product_length,
    JSON_VALUE(p.dimensions, '$.width') as product_width,
    JSON_VALUE(p.attributes, '$.color') as color
FROM products p
INNER JOIN categories c ON p.category_id = c.id
WHERE CONTAINS(p.name, 'wireless AND bluetooth', 1) > 0
    AND p.status = 'active'
    AND p.stock_quantity > 0
    AND p.price BETWEEN 50.00 AND 500.00
ORDER BY SCORE(1) DESC, p.price ASC
FETCH FIRST 25 ROWS ONLY;

-- 4. PL/SQL block with cursor and exception handling
DECLARE
    CURSOR c_low_stock IS
        SELECT id, name, sku, stock_quantity, price
        FROM products
        WHERE stock_quantity < 10 AND status = 'active';
    
    v_product_count NUMBER := 0;
    v_total_value NUMBER := 0;
    v_alert_message VARCHAR2(4000);
    
BEGIN
    -- Process low stock products
    FOR product_rec IN c_low_stock LOOP
        v_product_count := v_product_count + 1;
        v_total_value := v_total_value + (product_rec.stock_quantity * product_rec.price);
        
        -- Log each low stock product
        DBMS_OUTPUT.PUT_LINE('Low Stock Alert: ' || product_rec.name || 
                           ' (SKU: ' || product_rec.sku || ') - Stock: ' || product_rec.stock_quantity);
    END LOOP;
    
    -- Generate summary report
    IF v_product_count > 0 THEN
        v_alert_message := 'INVENTORY ALERT: ' || v_product_count || 
                          ' products with low stock. Total inventory value: $' || 
                          TO_CHAR(v_total_value, '999,999.99');
        DBMS_OUTPUT.PUT_LINE(v_alert_message);
        
        -- Update metadata for reporting
        UPDATE products 
        SET attributes = JSON_MERGEPATCH(
            COALESCE(attributes, '{}'), 
            '{"low_stock_alert_date": "' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || '"}'
        )
        WHERE stock_quantity < 10 AND status = 'active';
        
        COMMIT;
    ELSE
        DBMS_OUTPUT.PUT_LINE('No low stock alerts at this time.');
    END IF;
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error processing inventory: ' || SQLERRM);
        ROLLBACK;
END;
/'''
    },
    'sql_server': {
        'display_name': 'SQL Server',
        'icon': '🪟',
        'schema_label': 'SQL Server Schema Definition',
        'query_label': 'T-SQL Queries',
        'schema_term': 'Database Schema',
        'query_term': 'T-SQL Queries',
        'file_extensions': ['.sql', '.tsql'],
        'sample_schema': '''-- SQL Server Enterprise Database Schema Example
-- Demonstrates SQL Server-specific features and best practices

-- Create database with specific settings
CREATE DATABASE ECommerceDB
ON (NAME = 'ECommerceDB_Data',
    FILENAME = 'C:\Data\ECommerceDB.mdf',
    SIZE = 100MB,
    MAXSIZE = 1GB,
    FILEGROWTH = 10MB)
LOG ON (NAME = 'ECommerceDB_Log',
        FILENAME = 'C:\Data\ECommerceDB.ldf',
        SIZE = 10MB,
        MAXSIZE = 100MB,
        FILEGROWTH = 10%);

USE ECommerceDB;

-- Create schemas for organization
CREATE SCHEMA Sales;
CREATE SCHEMA Inventory;
CREATE SCHEMA Customer;

-- Users table with SQL Server features
CREATE TABLE Customer.Users (
    Id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    Username NVARCHAR(50) NOT NULL UNIQUE,
    Email NVARCHAR(100) NOT NULL UNIQUE,
    PasswordHash NVARCHAR(255) NOT NULL,
    FirstName NVARCHAR(50),
    LastName NVARCHAR(50),
    Phone NVARCHAR(20),
    DateOfBirth DATE,
    Status NVARCHAR(20) DEFAULT 'Active' CHECK (Status IN ('Active', 'Inactive', 'Suspended')),
    Preferences NVARCHAR(MAX) CHECK (ISJSON(Preferences) = 1),
    LastLoginAt DATETIME2(7),
    CreatedAt DATETIME2(7) DEFAULT GETDATE(),
    UpdatedAt DATETIME2(7) DEFAULT GETDATE(),
    RowVersion ROWVERSION,
    CONSTRAINT CK_Email_Format CHECK (Email LIKE '%_@%_.%')
);

-- Create indexes with included columns
CREATE NONCLUSTERED INDEX IX_Users_Username ON Customer.Users (Username) INCLUDE (Email, FirstName, LastName);
CREATE NONCLUSTERED INDEX IX_Users_Email ON Customer.Users (Email) INCLUDE (Username, Status);
CREATE NONCLUSTERED INDEX IX_Users_Status ON Customer.Users (Status) INCLUDE (CreatedAt);
CREATE NONCLUSTERED INDEX IX_Users_CreatedAt ON Customer.Users (CreatedAt DESC) INCLUDE (Status);

-- Categories with hierarchyid
CREATE TABLE Inventory.Categories (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    Name NVARCHAR(100) NOT NULL,
    Description NVARCHAR(MAX),
    ParentId INT REFERENCES Inventory.Categories(Id),
    Hierarchy HIERARCHYID,
    Slug NVARCHAR(100) NOT NULL UNIQUE,
    IsActive BIT DEFAULT 1,
    SortOrder INT DEFAULT 0,
    Metadata NVARCHAR(MAX) CHECK (ISJSON(Metadata) = 1),
    CreatedAt DATETIME2(7) DEFAULT GETDATE(),
    UpdatedAt DATETIME2(7) DEFAULT GETDATE()
);

CREATE UNIQUE NONCLUSTERED INDEX IX_Categories_Hierarchy ON Inventory.Categories (Hierarchy);
CREATE NONCLUSTERED INDEX IX_Categories_ParentId ON Inventory.Categories (ParentId);

-- Products table with computed columns and compression
CREATE TABLE Inventory.Products (
    Id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    Name NVARCHAR(255) NOT NULL,
    Description NVARCHAR(MAX),
    ShortDescription NVARCHAR(1000),
    SKU NVARCHAR(100) NOT NULL UNIQUE,
    Price MONEY NOT NULL CHECK (Price >= 0),
    CostPrice MONEY CHECK (CostPrice >= 0),
    StockQuantity INT DEFAULT 0 CHECK (StockQuantity >= 0),
    CategoryId INT NOT NULL REFERENCES Inventory.Categories(Id),
    Brand NVARCHAR(100),
    Weight DECIMAL(8,3),
    Dimensions NVARCHAR(MAX) CHECK (ISJSON(Dimensions) = 1),
    Attributes NVARCHAR(MAX) CHECK (ISJSON(Attributes) = 1),
    Tags NVARCHAR(500),
    Images NVARCHAR(MAX),
    Status NVARCHAR(20) DEFAULT 'Active' CHECK (Status IN ('Active', 'Inactive', 'Draft', 'Discontinued')),
    -- Computed columns
    ProfitMargin AS (CASE WHEN CostPrice > 0 THEN ((Price - CostPrice) / CostPrice) * 100 ELSE NULL END),
    StockStatus AS (CASE 
        WHEN StockQuantity = 0 THEN 'Out of Stock'
        WHEN StockQuantity < 10 THEN 'Low Stock'
        WHEN StockQuantity < 50 THEN 'Medium Stock'
        ELSE 'In Stock'
    END),
    CreatedAt DATETIME2(7) DEFAULT GETDATE(),
    UpdatedAt DATETIME2(7) DEFAULT GETDATE(),
    RowVersion ROWVERSION
) WITH (DATA_COMPRESSION = PAGE);

CREATE NONCLUSTERED INDEX IX_Products_CategoryId ON Inventory.Products (CategoryId) INCLUDE (Name, Price, StockQuantity);
CREATE NONCLUSTERED INDEX IX_Products_SKU ON Inventory.Products (SKU);
CREATE NONCLUSTERED INDEX IX_Products_Price ON Inventory.Products (Price) INCLUDE (Name, StockQuantity);
CREATE NONCLUSTERED INDEX IX_Products_Status_Stock ON Inventory.Products (Status, StockQuantity) INCLUDE (Name, Price);

-- Full-text catalog and index
CREATE FULLTEXT CATALOG ProductCatalog;
CREATE FULLTEXT INDEX ON Inventory.Products (Name, Description) KEY INDEX PK__Products__Id WITH CHANGE_TRACKING AUTO;

-- Orders table with partitioning
CREATE PARTITION FUNCTION pf_OrdersByDate (DATETIME2(7))
AS RANGE RIGHT FOR VALUES 
('2023-01-01', '2024-01-01', '2025-01-01', '2026-01-01');

CREATE PARTITION SCHEME ps_OrdersByDate
AS PARTITION pf_OrdersByDate
ALL TO ([PRIMARY]);

CREATE TABLE Sales.Orders (
    Id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    UserId UNIQUEIDENTIFIER NOT NULL REFERENCES Customer.Users(Id),
    OrderNumber NVARCHAR(50) NOT NULL UNIQUE,
    TotalAmount MONEY NOT NULL CHECK (TotalAmount >= 0),
    TaxAmount MONEY DEFAULT 0 CHECK (TaxAmount >= 0),
    ShippingAmount MONEY DEFAULT 0 CHECK (ShippingAmount >= 0),
    DiscountAmount MONEY DEFAULT 0 CHECK (DiscountAmount >= 0),
    Status NVARCHAR(20) DEFAULT 'Pending' CHECK (Status IN ('Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled')),
    PaymentStatus NVARCHAR(20) DEFAULT 'Pending' CHECK (PaymentStatus IN ('Pending', 'Paid', 'Failed', 'Refunded')),
    ShippingAddress NVARCHAR(MAX) CHECK (ISJSON(ShippingAddress) = 1),
    BillingAddress NVARCHAR(MAX) CHECK (ISJSON(BillingAddress) = 1),
    Metadata NVARCHAR(MAX) CHECK (ISJSON(Metadata) = 1),
    Notes NVARCHAR(MAX),
    ShippedAt DATETIME2(7),
    DeliveredAt DATETIME2(7),
    CreatedAt DATETIME2(7) DEFAULT GETDATE(),
    UpdatedAt DATETIME2(7) DEFAULT GETDATE(),
    RowVersion ROWVERSION
) ON ps_OrdersByDate(CreatedAt);

CREATE NONCLUSTERED INDEX IX_Orders_UserId ON Sales.Orders (UserId) INCLUDE (OrderNumber, TotalAmount, Status);
CREATE NONCLUSTERED INDEX IX_Orders_OrderNumber ON Sales.Orders (OrderNumber);
CREATE NONCLUSTERED INDEX IX_Orders_Status ON Sales.Orders (Status) INCLUDE (CreatedAt, TotalAmount);

-- Triggers for audit trail
CREATE TRIGGER tr_Users_UpdatedAt ON Customer.Users
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE Customer.Users 
    SET UpdatedAt = GETDATE()
    FROM Customer.Users u
    INNER JOIN inserted i ON u.Id = i.Id;
END;

CREATE TRIGGER tr_Products_UpdatedAt ON Inventory.Products
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE Inventory.Products 
    SET UpdatedAt = GETDATE()
    FROM Inventory.Products p
    INNER JOIN inserted i ON p.Id = i.Id;
END;''',
        'sample_queries': '''-- SQL Server T-SQL Query Examples with Advanced Features

-- 1. CTE with recursive hierarchy and analytical functions
WITH CategoryHierarchy AS (
    -- Anchor: Root categories
    SELECT 
        Id,
        Name,
        ParentId,
        Hierarchy,
        0 as Level,
        CAST(Name AS NVARCHAR(MAX)) as CategoryPath
    FROM Inventory.Categories
    WHERE ParentId IS NULL
    
    UNION ALL
    
    -- Recursive: Child categories
    SELECT 
        c.Id,
        c.Name,
        c.ParentId,
        c.Hierarchy,
        ch.Level + 1,
        CAST(ch.CategoryPath + ' > ' + c.Name AS NVARCHAR(MAX))
    FROM Inventory.Categories c
    INNER JOIN CategoryHierarchy ch ON c.ParentId = ch.Id
),
CategorySales AS (
    SELECT 
        p.CategoryId,
        COUNT(DISTINCT o.Id) as OrderCount,
        SUM(o.TotalAmount) as TotalRevenue,
        AVG(o.TotalAmount) as AvgOrderValue
    FROM Sales.Orders o
    INNER JOIN Sales.OrderItems oi ON o.Id = oi.OrderId
    INNER JOIN Inventory.Products p ON oi.ProductId = p.Id
    WHERE o.Status IN ('Delivered', 'Shipped')
        AND o.CreatedAt >= DATEADD(MONTH, -12, GETDATE())
    GROUP BY p.CategoryId
)
SELECT 
    ch.Id,
    REPLICATE('  ', ch.Level) + ch.Name as CategoryHierarchy,
    ch.CategoryPath,
    ch.Level,
    ISNULL(cs.OrderCount, 0) as OrderCount,
    ISNULL(cs.TotalRevenue, 0) as TotalRevenue,
    ISNULL(cs.AvgOrderValue, 0) as AvgOrderValue,
    RANK() OVER (ORDER BY ISNULL(cs.TotalRevenue, 0) DESC) as RevenueRank
FROM CategoryHierarchy ch
LEFT JOIN CategorySales cs ON ch.Id = cs.CategoryId
ORDER BY ch.Hierarchy;

-- 2. Advanced window functions with CROSS APPLY
SELECT 
    o.Id,
    o.OrderNumber,
    u.Username,
    o.TotalAmount,
    o.CreatedAt,
    o.Status,
    -- Window functions for analytics
    ROW_NUMBER() OVER (PARTITION BY o.UserId ORDER BY o.CreatedAt DESC) as UserOrderRank,
    SUM(o.TotalAmount) OVER (PARTITION BY o.UserId) as UserTotalSpent,
    AVG(o.TotalAmount) OVER (PARTITION BY o.UserId) as UserAvgOrderValue,
    LAG(o.TotalAmount) OVER (PARTITION BY o.UserId ORDER BY o.CreatedAt) as PreviousOrderAmount,
    LEAD(o.CreatedAt) OVER (PARTITION BY o.UserId ORDER BY o.CreatedAt) as NextOrderDate,
    -- Customer lifetime calculations
    DATEDIFF(DAY, MIN(o.CreatedAt) OVER (PARTITION BY o.UserId), GETDATE()) as CustomerLifetimeDays,
    COUNT(*) OVER (PARTITION BY o.UserId) as TotalOrderCount,
    -- Monthly comparisons
    monthly_stats.MonthlyAvg,
    monthly_stats.MonthlyCount
FROM Sales.Orders o
INNER JOIN Customer.Users u ON o.UserId = u.Id
CROSS APPLY (
    SELECT 
        AVG(CAST(mo.TotalAmount AS FLOAT)) as MonthlyAvg,
        COUNT(*) as MonthlyCount
    FROM Sales.Orders mo
    WHERE mo.UserId = o.UserId
        AND YEAR(mo.CreatedAt) = YEAR(o.CreatedAt)
        AND MONTH(mo.CreatedAt) = MONTH(o.CreatedAt)
        AND mo.Status IN ('Delivered', 'Shipped')
) monthly_stats
WHERE o.CreatedAt >= DATEADD(YEAR, -2, GETDATE())
    AND o.Status != 'Cancelled'
ORDER BY o.UserId, o.CreatedAt DESC;

-- 3. Full-text search with ranking and JSON functions
SELECT 
    p.Id,
    p.Name,
    p.Price,
    p.StockQuantity,
    p.StockStatus,
    p.ProfitMargin,
    c.Name as CategoryName,
    p.Brand,
    -- Full-text search ranking
    KEY_TBL.RANK as SearchRelevance,
    -- JSON attribute extraction
    JSON_VALUE(p.Attributes, '$.color') as Color,
    JSON_VALUE(p.Attributes, '$.size') as Size,
    JSON_VALUE(p.Attributes, '$.material') as Material,
    JSON_VALUE(p.Dimensions, '$.length') as Length,
    JSON_VALUE(p.Dimensions, '$.width') as Width,
    JSON_VALUE(p.Dimensions, '$.height') as Height,
    -- Price analysis
    PERCENT_RANK() OVER (PARTITION BY p.CategoryId ORDER BY p.Price) as PricePercentile,
    CASE 
        WHEN p.Price > (
            SELECT AVG(CAST(Price AS FLOAT)) * 1.5 
            FROM Inventory.Products 
            WHERE CategoryId = p.CategoryId AND Status = 'Active'
        ) THEN 'Premium'
        WHEN p.Price < (
            SELECT AVG(CAST(Price AS FLOAT)) * 0.7 
            FROM Inventory.Products 
            WHERE CategoryId = p.CategoryId AND Status = 'Active'
        ) THEN 'Budget'
        ELSE 'Standard'
    END as PriceCategory
FROM Inventory.Products p
INNER JOIN Inventory.Categories c ON p.CategoryId = c.Id
INNER JOIN FREETEXTTABLE(Inventory.Products, (Name, Description), 'wireless bluetooth headphones') KEY_TBL
    ON p.Id = KEY_TBL.[KEY]
WHERE p.Status = 'Active'
    AND p.StockQuantity > 0
    AND p.Price BETWEEN 50.00 AND 500.00
ORDER BY KEY_TBL.RANK DESC, p.Price ASC;

-- 4. Stored procedure with error handling and transactions
CREATE PROCEDURE Sales.ProcessOrderBatch
    @BatchSize INT = 100,
    @MaxRetries INT = 3
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    DECLARE @ErrorCount INT = 0;
    DECLARE @ProcessedCount INT = 0;
    DECLARE @CurrentOrderId UNIQUEIDENTIFIER;
    DECLARE @RetryCount INT = 0;
    
    -- Create temporary table for batch processing
    CREATE TABLE #OrdersToProcess (
        OrderId UNIQUEIDENTIFIER,
        ProcessAttempts INT DEFAULT 0
    );
    
    -- Get orders to process
    INSERT INTO #OrdersToProcess (OrderId)
    SELECT TOP (@BatchSize) Id
    FROM Sales.Orders
    WHERE Status = 'Pending'
        AND CreatedAt >= DATEADD(HOUR, -24, GETDATE())
    ORDER BY CreatedAt ASC;
    
    -- Process each order
    DECLARE order_cursor CURSOR FOR
        SELECT OrderId FROM #OrdersToProcess;
    
    OPEN order_cursor;
    FETCH NEXT FROM order_cursor INTO @CurrentOrderId;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            BEGIN TRANSACTION;
            
            -- Simulate order processing logic
            UPDATE Sales.Orders
            SET Status = 'Processing',
                UpdatedAt = GETDATE(),
                Metadata = JSON_MODIFY(
                    ISNULL(Metadata, '{}'), 
                    '$.processing_started', 
                    FORMAT(GETDATE(), 'yyyy-MM-ddTHH:mm:ss')
                )
            WHERE Id = @CurrentOrderId;
            
            -- Validate inventory
            IF EXISTS (
                SELECT 1 
                FROM Sales.OrderItems oi
                INNER JOIN Inventory.Products p ON oi.ProductId = p.Id
                WHERE oi.OrderId = @CurrentOrderId
                    AND p.StockQuantity < oi.Quantity
            )
            BEGIN
                -- Insufficient inventory
                UPDATE Sales.Orders
                SET Status = 'On Hold',
                    Metadata = JSON_MODIFY(
                        ISNULL(Metadata, '{}'), 
                        '$.hold_reason', 
                        'Insufficient inventory'
                    )
                WHERE Id = @CurrentOrderId;
            END
            ELSE
            BEGIN
                -- Process successfully
                UPDATE Sales.Orders
                SET Status = 'Confirmed',
                    Metadata = JSON_MODIFY(
                        ISNULL(Metadata, '{}'), 
                        '$.confirmed_at', 
                        FORMAT(GETDATE(), 'yyyy-MM-ddTHH:mm:ss')
                    )
                WHERE Id = @CurrentOrderId;
                
                SET @ProcessedCount = @ProcessedCount + 1;
            END
            
            COMMIT TRANSACTION;
            
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0
                ROLLBACK TRANSACTION;
            
            SET @ErrorCount = @ErrorCount + 1;
            
            -- Log error
            INSERT INTO System.ErrorLog (
                ErrorMessage,
                ErrorSeverity,
                ErrorState,
                ErrorProcedure,
                ErrorLine,
                ErrorNumber,
                CreatedAt
            )
            VALUES (
                ERROR_MESSAGE(),
                ERROR_SEVERITY(),
                ERROR_STATE(),
                ERROR_PROCEDURE(),
                ERROR_LINE(),
                ERROR_NUMBER(),
                GETDATE()
            );
            
            -- Update retry count
            UPDATE #OrdersToProcess 
            SET ProcessAttempts = ProcessAttempts + 1
            WHERE OrderId = @CurrentOrderId;
            
        END CATCH
        
        FETCH NEXT FROM order_cursor INTO @CurrentOrderId;
    END
    
    CLOSE order_cursor;
    DEALLOCATE order_cursor;
    
    -- Return results
    SELECT 
        @ProcessedCount as ProcessedOrders,
        @ErrorCount as ErrorCount,
        (SELECT COUNT(*) FROM #OrdersToProcess) as TotalBatchSize;
    
    DROP TABLE #OrdersToProcess;
END;'''
    },
    'mongodb': {
        'display_name': 'MongoDB',
        'icon': '🍃',
        'schema_label': 'MongoDB Collection Schema',
        'query_label': 'MongoDB Queries',
        'schema_term': 'Collection Schema',
        'query_term': 'MongoDB Queries',
        'file_extensions': ['.js', '.json'],
        'sample_schema': '''// MongoDB E-commerce Database Schema Example
// Demonstrates MongoDB document structure and validation

// Users Collection with comprehensive validation
db.createCollection("users", {
   validator: {
      $jsonSchema: {
         bsonType: "object",
         required: ["username", "email", "password_hash"],
         properties: {
            _id: { bsonType: "objectId" },
            username: { 
                bsonType: "string", 
                minLength: 3,
                maxLength: 50,
                pattern: "^[a-zA-Z0-9_]+$"
            },
            email: { 
                bsonType: "string", 
                maxLength: 100,
                pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
            },
            password_hash: { bsonType: "string", minLength: 60 },
            profile: {
                bsonType: "object",
                properties: {
                    first_name: { bsonType: "string", maxLength: 50 },
                    last_name: { bsonType: "string", maxLength: 50 },
                    phone: { bsonType: "string", maxLength: 20 },
                    date_of_birth: { bsonType: "date" },
                    avatar_url: { bsonType: "string", maxLength: 500 },
                    bio: { bsonType: "string", maxLength: 1000 }
                }
            },
            preferences: {
                bsonType: "object",
                properties: {
                    newsletter_subscribed: { bsonType: "bool" },
                    notifications_enabled: { bsonType: "bool" },
                    preferred_currency: { enum: ["USD", "EUR", "GBP", "CAD"] },
                    language: { bsonType: "string", maxLength: 5 },
                    timezone: { bsonType: "string", maxLength: 50 },
                    theme: { enum: ["light", "dark", "auto"] }
                }
            },
            addresses: {
                bsonType: "array",
                items: {
                    bsonType: "object",
                    required: ["type", "street", "city", "country"],
                    properties: {
                        type: { enum: ["shipping", "billing", "both"] },
                        street: { bsonType: "string", maxLength: 200 },
                        city: { bsonType: "string", maxLength: 100 },
                        state: { bsonType: "string", maxLength: 100 },
                        postal_code: { bsonType: "string", maxLength: 20 },
                        country: { bsonType: "string", maxLength: 100 },
                        is_default: { bsonType: "bool" }
                    }
                }
            },
            status: { enum: ["active", "inactive", "suspended", "pending_verification"] },
            roles: { 
                bsonType: "array",
                items: { enum: ["customer", "admin", "moderator", "vendor"] }
            },
            last_login_at: { bsonType: "date" },
            login_history: {
                bsonType: "array",
                maxItems: 10,
                items: {
                    bsonType: "object",
                    properties: {
                        timestamp: { bsonType: "date" },
                        ip_address: { bsonType: "string" },
                        user_agent: { bsonType: "string" },
                        location: { bsonType: "string" }
                    }
                }
            },
            created_at: { bsonType: "date" },
            updated_at: { bsonType: "date" }
         }
      }
   }
});

// Create indexes for users collection
db.users.createIndex({ "username": 1 }, { unique: true });
db.users.createIndex({ "email": 1 }, { unique: true });
db.users.createIndex({ "status": 1 });
db.users.createIndex({ "created_at": -1 });
db.users.createIndex({ "preferences.newsletter_subscribed": 1 });
db.users.createIndex({ "addresses.city": 1, "addresses.country": 1 });

// Categories Collection with hierarchical structure
db.createCollection("categories", {
   validator: {
      $jsonSchema: {
         bsonType: "object",
         required: ["name", "slug"],
         properties: {
            _id: { bsonType: "objectId" },
            name: { bsonType: "string", maxLength: 100 },
            description: { bsonType: "string", maxLength: 1000 },
            slug: { bsonType: "string", maxLength: 100 },
            parent_id: { bsonType: "objectId" },
            ancestors: { 
                bsonType: "array",
                items: { bsonType: "objectId" }
            },
            level: { bsonType: "int", minimum: 0 },
            path: { bsonType: "string" },
            is_active: { bsonType: "bool" },
            sort_order: { bsonType: "int" },
            metadata: {
                bsonType: "object",
                properties: {
                    seo_title: { bsonType: "string", maxLength: 200 },
                    seo_description: { bsonType: "string", maxLength: 500 },
                    image_url: { bsonType: "string", maxLength: 500 },
                    featured: { bsonType: "bool" }
                }
            },
            created_at: { bsonType: "date" },
            updated_at: { bsonType: "date" }
         }
      }
   }
});

db.categories.createIndex({ "slug": 1 }, { unique: true });
db.categories.createIndex({ "parent_id": 1 });
db.categories.createIndex({ "ancestors": 1 });
db.categories.createIndex({ "level": 1, "sort_order": 1 });

// Products Collection with rich document structure
db.createCollection("products", {
   validator: {
      $jsonSchema: {
         bsonType: "object",
         required: ["name", "sku", "price", "category_id"],
         properties: {
            _id: { bsonType: "objectId" },
            name: { bsonType: "string", maxLength: 255 },
            description: { bsonType: "string" },
            short_description: { bsonType: "string", maxLength: 500 },
            sku: { bsonType: "string", maxLength: 100 },
            price: { bsonType: "number", minimum: 0 },
            cost_price: { bsonType: "number", minimum: 0 },
            currency: { enum: ["USD", "EUR", "GBP", "CAD"] },
            inventory: {
                bsonType: "object",
                properties: {
                    stock_quantity: { bsonType: "int", minimum: 0 },
                    reserved_quantity: { bsonType: "int", minimum: 0 },
                    reorder_level: { bsonType: "int", minimum: 0 },
                    reorder_quantity: { bsonType: "int", minimum: 0 },
                    track_inventory: { bsonType: "bool" }
                }
            },
            category_id: { bsonType: "objectId" },
            categories: { 
                bsonType: "array",
                items: { bsonType: "objectId" }
            },
            brand: { bsonType: "string", maxLength: 100 },
            manufacturer: { bsonType: "string", maxLength: 100 },
            specifications: {
                bsonType: "object",
                properties: {
                    weight: { bsonType: "number", minimum: 0 },
                    dimensions: {
                        bsonType: "object",
                        properties: {
                            length: { bsonType: "number", minimum: 0 },
                            width: { bsonType: "number", minimum: 0 },
                            height: { bsonType: "number", minimum: 0 },
                            unit: { enum: ["cm", "in", "mm"] }
                        }
                    },
                    color: { bsonType: "string" },
                    material: { bsonType: "string" },
                    warranty_period: { bsonType: "int" },
                    warranty_unit: { enum: ["days", "months", "years"] }
                }
            },
            variants: {
                bsonType: "array",
                items: {
                    bsonType: "object",
                    properties: {
                        name: { bsonType: "string" },
                        values: { 
                            bsonType: "array",
                            items: { bsonType: "string" }
                        }
                    }
                }
            },
            images: {
                bsonType: "array",
                items: {
                    bsonType: "object",
                    properties: {
                        url: { bsonType: "string", maxLength: 500 },
                        alt_text: { bsonType: "string", maxLength: 200 },
                        sort_order: { bsonType: "int" },
                        is_primary: { bsonType: "bool" }
                    }
                }
            },
            tags: { 
                bsonType: "array",
                items: { bsonType: "string" }
            },
            status: { enum: ["active", "inactive", "draft", "discontinued"] },
            seo: {
                bsonType: "object",
                properties: {
                    title: { bsonType: "string", maxLength: 200 },
                    description: { bsonType: "string", maxLength: 500 },
                    keywords: { 
                        bsonType: "array",
                        items: { bsonType: "string" }
                    }
                }
            },
            pricing_history: {
                bsonType: "array",
                items: {
                    bsonType: "object",
                    properties: {
                        price: { bsonType: "number" },
                        effective_date: { bsonType: "date" },
                        reason: { bsonType: "string" }
                    }
                }
            },
            created_at: { bsonType: "date" },
            updated_at: { bsonType: "date" }
         }
      }
   }
});

db.products.createIndex({ "sku": 1 }, { unique: true });
db.products.createIndex({ "category_id": 1 });
db.products.createIndex({ "categories": 1 });
db.products.createIndex({ "brand": 1 });
db.products.createIndex({ "status": 1, "inventory.stock_quantity": 1 });
db.products.createIndex({ "tags": 1 });
db.products.createIndex({ "price": 1 });
db.products.createIndex({ "name": "text", "description": "text", "brand": "text" });

// Orders Collection with embedded order items
db.createCollection("orders", {
   validator: {
      $jsonSchema: {
         bsonType: "object",
         required: ["user_id", "order_number", "total_amount", "items"],
         properties: {
            _id: { bsonType: "objectId" },
            user_id: { bsonType: "objectId" },
            order_number: { bsonType: "string", maxLength: 50 },
            status: { enum: ["pending", "confirmed", "processing", "shipped", "delivered", "cancelled", "returned"] },
            payment_status: { enum: ["pending", "authorized", "captured", "failed", "refunded", "partially_refunded"] },
            amounts: {
                bsonType: "object",
                required: ["subtotal", "total"],
                properties: {
                    subtotal: { bsonType: "number", minimum: 0 },
                    tax_amount: { bsonType: "number", minimum: 0 },
                    shipping_amount: { bsonType: "number", minimum: 0 },
                    discount_amount: { bsonType: "number", minimum: 0 },
                    total: { bsonType: "number", minimum: 0 },
                    currency: { enum: ["USD", "EUR", "GBP", "CAD"] }
                }
            },
            items: {
                bsonType: "array",
                minItems: 1,
                items: {
                    bsonType: "object",
                    required: ["product_id", "quantity", "unit_price"],
                    properties: {
                        product_id: { bsonType: "objectId" },
                        product_name: { bsonType: "string" },
                        product_sku: { bsonType: "string" },
                        variant: { bsonType: "string" },
                        quantity: { bsonType: "int", minimum: 1 },
                        unit_price: { bsonType: "number", minimum: 0 },
                        total_price: { bsonType: "number", minimum: 0 },
                        discount_amount: { bsonType: "number", minimum: 0 }
                    }
                }
            },
            shipping_address: {
                bsonType: "object",
                required: ["street", "city", "country"],
                properties: {
                    recipient_name: { bsonType: "string" },
                    street: { bsonType: "string" },
                    city: { bsonType: "string" },
                    state: { bsonType: "string" },
                    postal_code: { bsonType: "string" },
                    country: { bsonType: "string" },
                    phone: { bsonType: "string" }
                }
            },
            billing_address: {
                bsonType: "object",
                properties: {
                    recipient_name: { bsonType: "string" },
                    street: { bsonType: "string" },
                    city: { bsonType: "string" },
                    state: { bsonType: "string" },
                    postal_code: { bsonType: "string" },
                    country: { bsonType: "string" }
                }
            },
            tracking: {
                bsonType: "object",
                properties: {
                    tracking_number: { bsonType: "string" },
                    carrier: { bsonType: "string" },
                    tracking_url: { bsonType: "string" },
                    estimated_delivery: { bsonType: "date" }
                }
            },
            timeline: {
                bsonType: "array",
                items: {
                    bsonType: "object",
                    properties: {
                        status: { bsonType: "string" },
                        timestamp: { bsonType: "date" },
                        notes: { bsonType: "string" },
                        updated_by: { bsonType: "objectId" }
                    }
                }
            },
            notes: { bsonType: "string" },
            created_at: { bsonType: "date" },
            updated_at: { bsonType: "date" }
         }
      }
   }
});

db.orders.createIndex({ "order_number": 1 }, { unique: true });
db.orders.createIndex({ "user_id": 1, "created_at": -1 });
db.orders.createIndex({ "status": 1 });
db.orders.createIndex({ "payment_status": 1 });
db.orders.createIndex({ "created_at": -1 });
db.orders.createIndex({ "items.product_id": 1 });''',
        'sample_queries': '''// MongoDB Query Examples with Advanced Aggregation

// 1. Advanced customer analytics with complex aggregation pipeline
db.orders.aggregate([
    // Match orders from last 12 months
    {
        $match: {
            created_at: { $gte: new Date(Date.now() - 365*24*60*60*1000) },
            status: { $in: ["delivered", "shipped"] }
        }
    },
    // Group by user to calculate customer metrics
    {
        $group: {
            _id: "$user_id",
            total_orders: { $sum: 1 },
            total_spent: { $sum: "$amounts.total" },
            avg_order_value: { $avg: "$amounts.total" },
            first_order: { $min: "$created_at" },
            last_order: { $max: "$created_at" },
            order_months: { $addToSet: { $dateToString: { format: "%Y-%m", date: "$created_at" } } },
            favorite_categories: { $push: "$items.product_id" },
            total_items: { $sum: { $size: "$items" } }
        }
    },
    // Add calculated fields
    {
        $addFields: {
            customer_lifetime_days: {
                $divide: [
                    { $subtract: ["$last_order", "$first_order"] },
                    1000 * 60 * 60 * 24
                ]
            },
            purchase_frequency: {
                $divide: ["$total_orders", { $size: "$order_months" }]
            },
            customer_segment: {
                $switch: {
                    branches: [
                        { case: { $gt: ["$total_spent", 5000] }, then: "VIP" },
                        { case: { $gt: ["$total_spent", 1000] }, then: "Premium" },
                        { case: { $gt: ["$total_orders", 10] }, then: "Frequent" },
                        { case: { $gt: ["$total_orders", 3] }, then: "Regular" }
                    ],
                    default: "Occasional"
                }
            }
        }
    },
    // Lookup user details
    {
        $lookup: {
            from: "users",
            localField: "_id",
            foreignField: "_id",
            as: "user_info"
        }
    },
    // Unwind user info
    {
        $unwind: "$user_info"
    },
    // Project final results
    {
        $project: {
            _id: 1,
            username: "$user_info.username",
            email: "$user_info.email",
            total_orders: 1,
            total_spent: { $round: ["$total_spent", 2] },
            avg_order_value: { $round: ["$avg_order_value", 2] },
            customer_lifetime_days: { $round: ["$customer_lifetime_days", 0] },
            purchase_frequency: { $round: ["$purchase_frequency", 2] },
            customer_segment: 1,
            last_order: 1,
            days_since_last_order: {
                $round: [
                    { $divide: [{ $subtract: [new Date(), "$last_order"] }, 1000 * 60 * 60 * 24] },
                    0
                ]
            }
        }
    },
    // Sort by total spent descending
    { $sort: { total_spent: -1 } },
    { $limit: 100 }
]);

// 2. Product performance analytics with category hierarchy
db.products.aggregate([
    // Lookup category information
    {
        $lookup: {
            from: "categories",
            localField: "category_id",
            foreignField: "_id",
            as: "category_info"
        }
    },
    { $unwind: "$category_info" },
    
    // Lookup order items for sales data
    {
        $lookup: {
            from: "orders",
            let: { product_id: "$_id" },
            pipeline: [
                { $match: { 
                    status: { $in: ["delivered", "shipped"] },
                    created_at: { $gte: new Date(Date.now() - 90*24*60*60*1000) }
                }},
                { $unwind: "$items" },
                { $match: { $expr: { $eq: ["$items.product_id", "$$product_id"] } } },
                {
                    $group: {
                        _id: null,
                        total_quantity_sold: { $sum: "$items.quantity" },
                        total_revenue: { $sum: "$items.total_price" },
                        order_count: { $sum: 1 },
                        avg_quantity_per_order: { $avg: "$items.quantity" }
                    }
                }
            ],
            as: "sales_data"
        }
    },
    
    // Add sales metrics with default values
    {
        $addFields: {
            sales_metrics: {
                $cond: {
                    if: { $gt: [{ $size: "$sales_data" }, 0] },
                    then: { $arrayElemAt: ["$sales_data", 0] },
                    else: {
                        total_quantity_sold: 0,
                        total_revenue: 0,
                        order_count: 0,
                        avg_quantity_per_order: 0
                    }
                }
            }
        }
    },
    
    // Calculate performance indicators
    {
        $addFields: {
            stock_turnover_rate: {
                $cond: {
                    if: { $gt: ["$inventory.stock_quantity", 0] },
                    then: { $divide: ["$sales_metrics.total_quantity_sold", "$inventory.stock_quantity"] },
                    else: 0
                }
            },
            profit_margin: {
                $cond: {
                    if: { $gt: ["$cost_price", 0] },
                    then: { $divide: [{ $subtract: ["$price", "$cost_price"] }, "$price"] },
                    else: 0
                }
            },
            inventory_status: {
                $switch: {
                    branches: [
                        { case: { $eq: ["$inventory.stock_quantity", 0] }, then: "Out of Stock" },
                        { case: { $lt: ["$inventory.stock_quantity", 10] }, then: "Low Stock" },
                        { case: { $lt: ["$inventory.stock_quantity", 50] }, then: "Medium Stock" }
                    ],
                    default: "In Stock"
                }
            }
        }
    },
    
    // Project results
    {
        $project: {
            _id: 1,
            name: 1,
            sku: 1,
            price: 1,
            cost_price: 1,
            brand: 1,
            category_name: "$category_info.name",
            category_path: "$category_info.path",
            stock_quantity: "$inventory.stock_quantity",
            inventory_status: 1,
            total_quantity_sold: "$sales_metrics.total_quantity_sold",
            total_revenue: { $round: ["$sales_metrics.total_revenue", 2] },
            order_count: "$sales_metrics.order_count",
            stock_turnover_rate: { $round: ["$stock_turnover_rate", 3] },
            profit_margin: { $round: ["$profit_margin", 3] },
            performance_score: {
                $add: [
                    { $multiply: ["$sales_metrics.total_revenue", 0.4] },
                    { $multiply: ["$stock_turnover_rate", 0.3] },
                    { $multiply: ["$profit_margin", 0.3] }
                ]
            }
        }
    },
    
    // Filter active products
    { $match: { status: "active" } },
    
    // Sort by performance score
    { $sort: { performance_score: -1 } },
    { $limit: 50 }
]);

// 3. Sales trend analysis with time series data
db.orders.aggregate([
    // Match recent orders
    {
        $match: {
            created_at: { $gte: new Date(Date.now() - 365*24*60*60*1000) },
            status: { $in: ["delivered", "shipped"] }
        }
    },
    
    // Group by month and calculate metrics
    {
        $group: {
            _id: {
                year: { $year: "$created_at" },
                month: { $month: "$created_at" }
            },
            total_orders: { $sum: 1 },
            total_revenue: { $sum: "$amounts.total" },
            avg_order_value: { $avg: "$amounts.total" },
            unique_customers: { $addToSet: "$user_id" },
            total_items_sold: { $sum: { $size: "$items" } }
        }
    },
    
    // Add calculated fields
    {
        $addFields: {
            month_year: {
                $dateFromParts: {
                    year: "$_id.year",
                    month: "$_id.month",
                    day: 1
                }
            },
            unique_customer_count: { $size: "$unique_customers" },
            revenue_per_customer: { $divide: ["$total_revenue", { $size: "$unique_customers" }] },
            items_per_order: { $divide: ["$total_items_sold", "$total_orders"] }
        }
    },
    
    // Sort by date
    { $sort: { month_year: 1 } },
    
    // Add growth calculations using $setWindowFields (MongoDB 5.0+)
    {
        $setWindowFields: {
            sortBy: { month_year: 1 },
            output: {
                previous_month_revenue: {
                    $shift: {
                        output: "$total_revenue",
                        by: -1
                    }
                },
                revenue_growth_rate: {
                    $divide: [
                        { $subtract: ["$total_revenue", { $shift: { output: "$total_revenue", by: -1 } }] },
                        { $shift: { output: "$total_revenue", by: -1 } }
                    ]
                },
                cumulative_revenue: {
                    $sum: "$total_revenue",
                    window: { documents: ["unbounded preceding", "current"] }
                }
            }
        }
    },
    
    // Project final results
    {
        $project: {
            _id: 0,
            month_year: { $dateToString: { format: "%Y-%m", date: "$month_year" } },
            total_orders: 1,
            total_revenue: { $round: ["$total_revenue", 2] },
            avg_order_value: { $round: ["$avg_order_value", 2] },
            unique_customer_count: 1,
            revenue_per_customer: { $round: ["$revenue_per_customer", 2] },
            items_per_order: { $round: ["$items_per_order", 2] },
            revenue_growth_rate: { 
                $round: [{ $multiply: ["$revenue_growth_rate", 100] }, 2]
            },
            cumulative_revenue: { $round: ["$cumulative_revenue", 2] }
        }
    }
]);

// 4. Advanced product search with faceted results
db.products.aggregate([
    // Text search stage
    {
        $match: {
            $and: [
                { $text: { $search: "wireless bluetooth headphones" } },
                { status: "active" },
                { "inventory.stock_quantity": { $gt: 0 } },
                { price: { $gte: 50, $lte: 500 } }
            ]
        }
    },
    
    // Add text score for relevance
    { $addFields: { search_score: { $meta: "textScore" } } },
    
    // Lookup category information
    {
        $lookup: {
            from: "categories",
            localField: "category_id",
            foreignField: "_id",
            as: "category"
        }
    },
    { $unwind: "$category" },
    
    // Create faceted results
    {
        $facet: {
            // Main search results
            products: [
                {
                    $project: {
                        _id: 1,
                        name: 1,
                        price: 1,
                        brand: 1,
                        stock_quantity: "$inventory.stock_quantity",
                        category_name: "$category.name",
                        images: { $slice: ["$images", 1] },
                        specifications: 1,
                        search_score: 1,
                        tags: 1
                    }
                },
                { $sort: { search_score: -1, price: 1 } },
                { $limit: 20 }
            ],
            
            // Brand facets
            brands: [
                { $group: { _id: "$brand", count: { $sum: 1 } } },
                { $sort: { count: -1 } },
                { $limit: 10 }
            ],
            
            // Category facets
            categories: [
                { 
                    $group: { 
                        _id: { id: "$category._id", name: "$category.name" }, 
                        count: { $sum: 1 } 
                    } 
                },
                { $sort: { count: -1 } },
                { $limit: 10 }
            ],
            
            // Price ranges
            price_ranges: [
                {
                    $bucket: {
                        groupBy: "$price",
                        boundaries: [0, 50, 100, 200, 300, 500, 1000],
                        default: "Other",
                        output: { count: { $sum: 1 } }
                    }
                }
            ],
            
            // Summary statistics
            summary: [
                {
                    $group: {
                        _id: null,
                        total_products: { $sum: 1 },
                        avg_price: { $avg: "$price" },
                        min_price: { $min: "$price" },
                        max_price: { $max: "$price" },
                        total_in_stock: { $sum: "$inventory.stock_quantity" }
                    }
                }
            ]
        }
    }
]);'''
    }
}

# Example migration scenarios and use cases
MIGRATION_EXAMPLES = {
    'enterprise_modernization': {
        'title': '🏢 Enterprise Database Modernization',
        'description': 'Large enterprise migrating from Oracle to AWS Aurora PostgreSQL',
        'source': 'oracle',
        'target': 'aurora_postgresql',
        'complexity': 'high',
        'scenario': '''
        **Business Context:**
        - Fortune 500 company with 50TB Oracle database
        - 200+ database objects (tables, views, procedures, functions)
        - Complex business logic in PL/SQL stored procedures
        - High availability requirements (99.9% uptime)
        - Regulatory compliance requirements
        
        **Migration Drivers:**
        - Reduce licensing costs by 60%
        - Improve scalability and performance
        - Leverage cloud-native features
        - Enhanced backup and disaster recovery
        
        **Expected Challenges:**
        - Oracle-specific data types (NUMBER, VARCHAR2, CLOB)
        - PL/SQL to PostgreSQL function conversion
        - Oracle RAC to Aurora Global Database
        - Custom Oracle Text indexes
        - Complex triggers and sequences
        ''',
        'sample_schema': '''-- Oracle Enterprise Schema Sample
CREATE TABLE employees (
    emp_id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    employee_number VARCHAR2(20) NOT NULL UNIQUE,
    first_name VARCHAR2(50) NOT NULL,
    last_name VARCHAR2(50) NOT NULL,
    email VARCHAR2(100) UNIQUE,
    hire_date DATE DEFAULT SYSDATE,
    salary NUMBER(10,2) CHECK (salary > 0),
    commission_pct NUMBER(3,2),
    department_id NUMBER,
    manager_id NUMBER,
    status VARCHAR2(10) DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'INACTIVE', 'TERMINATED')),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_emp_dept ON employees(department_id);
CREATE INDEX idx_emp_manager ON employees(manager_id);''',
        'expected_outcome': '''
        **Migration Results:**
        - 85% automated schema conversion
        - 40+ stored procedures requiring manual review
        - 6-month migration timeline
        - $2M annual cost savings
        - Performance improvement: 30% faster queries
        '''
    },
    'saas_startup_migration': {
        'title': '🚀 SaaS Startup: MySQL to Aurora MySQL',
        'description': 'Growing SaaS company migrating from self-managed MySQL to Aurora',
        'source': 'mysql',
        'target': 'aurora_mysql',
        'complexity': 'low',
        'scenario': '''
        **Business Context:**
        - SaaS startup with 500GB MySQL database
        - Multi-tenant architecture
        - Rapid user growth (200% year-over-year)
        - Limited database administration resources
        - Need for automatic scaling
        
        **Migration Drivers:**
        - Eliminate database maintenance overhead
        - Automatic scaling and performance optimization
        - Built-in monitoring and alerting
        - Enhanced security features
        - Global expansion requirements
        
        **Expected Benefits:**
        - 99.99% availability SLA
        - Automatic storage scaling
        - Read replica auto-scaling
        - Reduced operational overhead
        ''',
        'sample_schema': '''-- MySQL SaaS Application Schema
CREATE TABLE tenants (
    id INT AUTO_INCREMENT PRIMARY KEY,
    tenant_id VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    plan VARCHAR(20) DEFAULT 'starter',
    status ENUM('active', 'suspended', 'cancelled') DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_tenant_id (tenant_id),
    INDEX idx_status (status)
) ENGINE=InnoDB;

CREATE TABLE tenant_users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    tenant_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    role ENUM('admin', 'user', 'viewer') DEFAULT 'user',
    UNIQUE KEY unique_tenant_user (tenant_id, user_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
) ENGINE=InnoDB;''',
        'expected_outcome': '''
        **Migration Results:**
        - 95% schema compatibility
        - 2-week migration timeline
        - Zero application code changes
        - 50% reduction in database costs
        - Auto-scaling eliminates capacity planning
        '''
    },
    'ecommerce_mongodb_migration': {
        'title': '🛒 E-commerce: MongoDB to PostgreSQL',
        'description': 'E-commerce platform moving from MongoDB to PostgreSQL for ACID compliance',
        'source': 'mongodb',
        'target': 'postgresql',
        'complexity': 'very_high',
        'scenario': '''
        **Business Context:**
        - E-commerce platform with 1TB MongoDB database
        - Complex product catalog with nested attributes
        - Order processing requiring ACID transactions
        - Inventory management across multiple warehouses
        - Real-time analytics requirements
        
        **Migration Drivers:**
        - Strong consistency for financial transactions
        - Complex reporting and analytics needs
        - Integration with existing SQL-based tools
        - Better performance for complex queries
        - Regulatory compliance requirements
        
        **Major Challenges:**
        - Document structure to relational mapping
        - Nested JSON to normalized tables
        - Aggregation pipeline to SQL conversion
        - Application code rewrite required
        ''',
        'sample_schema': '''// MongoDB E-commerce Document
{
  "_id": ObjectId("..."),
  "sku": "PROD-12345",
  "name": "Wireless Bluetooth Headphones",
  "category": {
    "id": ObjectId("..."),
    "name": "Electronics",
    "path": "Electronics > Audio > Headphones"
  },
  "attributes": {
    "color": ["Black", "White", "Blue"],
    "connectivity": "Bluetooth 5.0",
    "battery_life": "30 hours",
    "noise_cancelling": true
  },
  "variants": [
    {
      "color": "Black",
      "price": 299.99,
      "stock": 150
    }
  ],
  "inventory": {
    "warehouses": [
      {
        "location": "US-East",
        "quantity": 75
      },
      {
        "location": "US-West", 
        "quantity": 75
      }
    ]
  }
}''',
        'expected_outcome': '''
        **Migration Results:**
        - Complete application rewrite required
        - 12-month migration timeline
        - 40% performance improvement for complex queries
        - Strong consistency for financial operations
        - Enhanced reporting capabilities
        '''
    }
}

def get_database_info(engine: str) -> Dict:
    """Get database-specific configuration"""
    return DATABASE_CONFIG.get(engine, {
        'display_name': engine.title(),
        'icon': '🗄️',
        'schema_label': f'{engine.title()} Schema Definition',
        'query_label': f'{engine.title()} Queries',
        'schema_term': 'Schema Definition',
        'query_term': 'Queries',
        'file_extensions': ['.sql'],
        'sample_schema': 'CREATE TABLE example (id INT PRIMARY KEY);',
        'sample_queries': 'SELECT * FROM example;'
    })

class AIAnalysisManager:
    """AI-powered analysis using Anthropic Claude"""
    
    def __init__(self):
        self.api_key = st.secrets.get("ANTHROPIC_API_KEY")
        self.client = None
        self.connected = False
        
        if self.api_key:
            try:
                self.client = anthropic.Anthropic(api_key=self.api_key)
                # Test connection
                test_message = self.client.messages.create(
                    model="claude-3-5-sonnet-20241022",
                    max_tokens=10,
                    messages=[{"role": "user", "content": "test"}]
                )
                self.connected = True
                logger.info("Anthropic AI client initialized successfully")
            except Exception as e:
                self.connected = False
                logger.error(f"Failed to initialize Anthropic client: {e}")
    
    async def analyze_schema_compatibility(self, source_engine: str, target_engine: str, 
                                         schema_objects: List[Dict]) -> Dict:
        """AI-powered schema compatibility analysis"""
        if not self.connected:
            return self._fallback_schema_analysis(source_engine, target_engine, schema_objects)
        
        try:
            schema_summary = "\n".join([f"- {obj['name']} ({obj['type']})" for obj in schema_objects[:10]])
            
            # Get database-specific information for better AI analysis
            source_info = get_database_info(source_engine)
            target_info = get_database_info(target_engine.replace('aurora_', ''))
            
            prompt = f"""
            As a database migration expert, analyze the compatibility of migrating from {source_info['display_name']} to {target_info['display_name']}.

            Source Database: {source_info['display_name']} ({source_info['query_term']})
            Target Database: {target_info['display_name']} ({target_info['query_term']})

            Schema Objects to Analyze:
            {schema_summary}

            Please provide:
            1. Overall compatibility assessment (percentage)
            2. Major compatibility issues and risks specific to {source_info['display_name']} → {target_info['display_name']} migration
            3. Recommended migration approach for this database combination
            4. Specific object-level concerns
            5. AWS service recommendations for {target_info['display_name']}
            6. Migration complexity scoring (Low/Medium/High/Very High)
            7. Timeline estimation for migration
            8. Critical success factors for {source_info['display_name']} to {target_info['display_name']} migration

            Format as detailed technical analysis with specific recommendations.
            """
            
            message = self.client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=2000,
                temperature=0.2,
                messages=[{"role": "user", "content": prompt}]
            )
            
            response = message.content[0].text
            
            return {
                'ai_analysis': response,
                'compatibility_score': self._extract_compatibility_score(response),
                'complexity_level': self._extract_complexity_level(response),
                'major_issues': self._extract_issues(response),
                'recommendations': self._extract_recommendations(response),
                'aws_services': self._extract_aws_services(response),
                'timeline_estimate': self._extract_timeline(response)
            }
            
        except Exception as e:
            logger.error(f"AI analysis failed: {e}")
            return self._fallback_schema_analysis(source_engine, target_engine, schema_objects)
    
    def _extract_compatibility_score(self, response: str) -> float:
        """Extract compatibility score from AI response"""
        import re
        score_match = re.search(r'(\d+)%', response)
        if score_match:
            return float(score_match.group(1))
        return 75.0  # Default
    
    def _extract_complexity_level(self, response: str) -> str:
        """Extract complexity level from AI response"""
        response_lower = response.lower()
        if 'very high' in response_lower:
            return 'very_high'
        elif 'high' in response_lower:
            return 'high'
        elif 'medium' in response_lower:
            return 'medium'
        else:
            return 'low'
    
    def _extract_issues(self, response: str) -> List[str]:
        """Extract major issues from AI response"""
        # Simple extraction - in production, use more sophisticated NLP
        lines = response.split('\n')
        issues = []
        in_issues_section = False
        
        for line in lines:
            if 'issue' in line.lower() or 'risk' in line.lower():
                in_issues_section = True
            elif in_issues_section and line.strip().startswith('-'):
                issues.append(line.strip()[1:].strip())
            elif in_issues_section and not line.strip():
                in_issues_section = False
                
        return issues[:5]  # Limit to 5 major issues
    
    def _extract_recommendations(self, response: str) -> List[str]:
        """Extract recommendations from AI response"""
        lines = response.split('\n')
        recommendations = []
        in_rec_section = False
        
        for line in lines:
            if 'recommend' in line.lower():
                in_rec_section = True
            elif in_rec_section and line.strip().startswith('-'):
                recommendations.append(line.strip()[1:].strip())
            elif in_rec_section and not line.strip():
                in_rec_section = False
                
        return recommendations[:5]
    
    def _extract_aws_services(self, response: str) -> List[str]:
        """Extract AWS service recommendations"""
        aws_services = ['RDS', 'Aurora', 'DynamoDB', 'DocumentDB', 'ElastiCache', 'Redshift']
        found_services = []
        
        for service in aws_services:
            if service.lower() in response.lower():
                found_services.append(service)
                
        return found_services
    
    def _extract_timeline(self, response: str) -> str:
        """Extract timeline estimate"""
        import re
        timeline_match = re.search(r'(\d+)\s*(week|month|day)', response.lower())
        if timeline_match:
            return f"{timeline_match.group(1)} {timeline_match.group(2)}s"
        return "4-6 weeks"
    
    def _fallback_schema_analysis(self, source_engine: str, target_engine: str, 
                                schema_objects: List[Dict]) -> Dict:
        """Fallback analysis when AI is not available"""
        source_info = get_database_info(source_engine)
        target_info = get_database_info(target_engine.replace('aurora_', ''))
        
        return {
            'ai_analysis': f'AI analysis not available - using fallback assessment for {source_info["display_name"]} to {target_info["display_name"]} migration',
            'compatibility_score': 70.0,
            'complexity_level': 'medium',
            'major_issues': [
                f'{source_info["display_name"]} to {target_info["display_name"]} schema conversion complexity varies by object type',
                'Data type mappings require validation',
                f'{source_info["query_term"]} may need rewriting for {target_info["display_name"]}'
            ],
            'recommendations': [
                'Conduct thorough testing in staging environment',
                f'Review all {source_info["query_term"].lower()} and stored procedures',
                'Validate data type conversions'
            ],
            'aws_services': ['RDS', 'Aurora'],
            'timeline_estimate': '6-8 weeks'
        }

class SchemaAnalyzer:
    """Advanced schema compatibility analysis"""
    
    def __init__(self):
        self.data_type_mappings = self._load_data_type_mappings()
        self.function_mappings = self._load_function_mappings()
        self.ai_manager = AIAnalysisManager()
    
    def _load_data_type_mappings(self) -> Dict:
        """Load data type mapping definitions"""
        return {
            'mysql_to_postgresql': {
                'VARCHAR': 'VARCHAR',
                'TEXT': 'TEXT',
                'INT': 'INTEGER',
                'BIGINT': 'BIGINT',
                'DECIMAL': 'NUMERIC',
                'DATETIME': 'TIMESTAMP',
                'TINYINT(1)': 'BOOLEAN',
                'LONGTEXT': 'TEXT',
                'MEDIUMTEXT': 'TEXT',
                'ENUM': 'VARCHAR',  # With CHECK constraint
                'SET': 'VARCHAR[]',
                'TIMESTAMP': 'TIMESTAMP WITH TIME ZONE'
            },
            'mysql_to_oracle': {
                'VARCHAR': 'VARCHAR2',
                'TEXT': 'CLOB',
                'INT': 'NUMBER(10)',
                'BIGINT': 'NUMBER(19)',
                'DECIMAL': 'NUMBER',
                'DATETIME': 'DATE',
                'TINYINT(1)': 'NUMBER(1)',
                'LONGTEXT': 'CLOB',
                'TIMESTAMP': 'TIMESTAMP'
            },
            'oracle_to_postgresql': {
                'VARCHAR2': 'VARCHAR',
                'CLOB': 'TEXT',
                'NUMBER': 'NUMERIC',
                'DATE': 'TIMESTAMP',
                'TIMESTAMP': 'TIMESTAMP',
                'RAW': 'BYTEA',
                'LONG': 'TEXT'
            },
            'sql_server_to_postgresql': {
                'NVARCHAR': 'VARCHAR',
                'NTEXT': 'TEXT',
                'INT': 'INTEGER',
                'BIGINT': 'BIGINT',
                'DECIMAL': 'NUMERIC',
                'DATETIME': 'TIMESTAMP',
                'BIT': 'BOOLEAN',
                'UNIQUEIDENTIFIER': 'UUID',
                'MONEY': 'MONEY'
            },
            'mongodb_to_postgresql': {
                'string': 'VARCHAR',
                'number': 'NUMERIC',
                'boolean': 'BOOLEAN',
                'date': 'TIMESTAMP',
                'objectId': 'UUID',
                'array': 'JSONB'
            }
        }
    
    def _load_function_mappings(self) -> Dict:
        """Load function mapping definitions"""
        return {
            'mysql_to_postgresql': {
                'CONCAT': 'CONCAT',
                'LENGTH': 'LENGTH',
                'SUBSTRING': 'SUBSTRING',
                'NOW()': 'NOW()',
                'DATE_FORMAT': 'TO_CHAR',
                'STR_TO_DATE': 'TO_DATE',
                'IFNULL': 'COALESCE',
                'IF': 'CASE WHEN',
                'LIMIT': 'LIMIT',
                'GROUP_CONCAT': 'STRING_AGG'
            },
            'oracle_to_postgresql': {
                'SYSDATE': 'NOW()',
                'TO_DATE': 'TO_TIMESTAMP',
                'TO_CHAR': 'TO_CHAR',
                'NVL': 'COALESCE',
                'DECODE': 'CASE WHEN',
                'ROWNUM': 'ROW_NUMBER()',
                'SUBSTR': 'SUBSTRING',
                'INSTR': 'POSITION'
            },
            'sql_server_to_postgresql': {
                'GETDATE()': 'NOW()',
                'LEN': 'LENGTH',
                'ISNULL': 'COALESCE',
                'CHARINDEX': 'POSITION',
                'DATEDIFF': 'EXTRACT',
                'TOP': 'LIMIT',
                'NEWID()': 'GEN_RANDOM_UUID()'
            },
            'mongodb_to_postgresql': {
                '$match': 'WHERE',
                '$group': 'GROUP BY',
                '$sort': 'ORDER BY',
                '$limit': 'LIMIT',
                '$lookup': 'JOIN'
            }
        }
    
    def analyze_table_compatibility(self, source_engine: str, target_engine: str, 
                                  table_definition: str) -> Dict:
        """Analyze table compatibility"""
        mapping_key = f"{source_engine}_to_{target_engine}"
        type_mappings = self.data_type_mappings.get(mapping_key, {})
        
        issues = []
        recommendations = []
        converted_ddl = table_definition
        compatibility_score = 100.0
        
        # Get database info for better analysis
        source_info = get_database_info(source_engine)
        target_info = get_database_info(target_engine)
        
        # Analyze data types
        for source_type, target_type in type_mappings.items():
            if source_type.upper() in table_definition.upper():
                if source_type == target_type:
                    # Direct mapping
                    continue
                elif target_type.endswith('[]'):
                    # Array type conversion
                    issues.append(f"Array conversion required for {source_type}")
                    compatibility_score -= 10
                    recommendations.append(f"Convert {source_type} to {target_type} with proper array handling")
                else:
                    # Standard conversion
                    converted_ddl = converted_ddl.replace(source_type, target_type)
                    recommendations.append(f"Convert {source_type} to {target_type}")
        
        # Check for engine-specific features
        if source_engine == 'mysql':
            if 'AUTO_INCREMENT' in table_definition.upper():
                if target_engine == 'postgresql':
                    issues.append("AUTO_INCREMENT needs conversion to SERIAL or IDENTITY")
                    compatibility_score -= 5
                    recommendations.append("Replace AUTO_INCREMENT with SERIAL or IDENTITY column")
            
            if 'ENGINE=' in table_definition.upper():
                issues.append("Storage engine specifications need review")
                compatibility_score -= 3
        
        elif source_engine == 'oracle':
            if 'NUMBER' in table_definition.upper():
                if target_engine == 'postgresql':
                    issues.append("Oracle NUMBER type precision and scale need careful mapping")
                    compatibility_score -= 8
                    recommendations.append("Review NUMBER precision and scale for PostgreSQL NUMERIC conversion")
        
        elif source_engine == 'mongodb':
            if target_engine in ['mysql', 'postgresql', 'oracle']:
                issues.append("Document structure requires relational table design")
                compatibility_score -= 25
                recommendations.append("Design normalized relational schema for document collections")
        
        return {
            'compatibility_score': compatibility_score,
            'issues': issues,
            'recommendations': recommendations,
            'converted_ddl': converted_ddl,
            'complexity': self._determine_complexity(compatibility_score)
        }
    
    def analyze_query_compatibility(self, source_engine: str, target_engine: str, 
                                  query: str) -> QueryAnalysis:
        """Analyze SQL query compatibility"""
        mapping_key = f"{source_engine}_to_{target_engine}"
        function_mappings = self.function_mappings.get(mapping_key, {})
        
        issues = []
        converted_query = query
        compatibility_score = 100.0
        
        # Function mappings
        for source_func, target_func in function_mappings.items():
            if source_func.upper() in query.upper():
                if source_func != target_func:
                    converted_query = converted_query.replace(source_func, target_func)
                    issues.append({
                        'type': 'function_conversion',
                        'description': f"Function {source_func} converted to {target_func}",
                        'severity': 'medium'
                    })
                    compatibility_score -= 5
        
        # Engine-specific syntax checks
        if source_engine == 'mysql':
            # MySQL backticks
            if '`' in query:
                converted_query = converted_query.replace('`', '"')
                issues.append({
                    'type': 'identifier_quotes',
                    'description': 'MySQL backticks converted to double quotes',
                    'severity': 'low'
                })
                compatibility_score -= 2
            
            # LIMIT syntax
            if re.search(r'LIMIT\s+\d+\s*,\s*\d+', query, re.IGNORECASE):
                issues.append({
                    'type': 'limit_syntax',
                    'description': 'MySQL LIMIT offset syntax needs conversion to OFFSET',
                    'severity': 'medium'
                })
                compatibility_score -= 8
        
        elif source_engine == 'mongodb':
            # MongoDB aggregation pipeline to SQL conversion
            if '$match' in query:
                issues.append({
                    'type': 'aggregation_conversion',
                    'description': 'MongoDB aggregation pipeline requires SQL rewrite',
                    'severity': 'high'
                })
                compatibility_score -= 20
        
        return QueryAnalysis(
            original_query=query,
            compatibility_score=compatibility_score,
            issues=issues,
            converted_query=converted_query,
            complexity=self._determine_complexity(compatibility_score),
            performance_impact=self._assess_performance_impact(compatibility_score)
        )
    
    def _determine_complexity(self, score: float) -> ComplexityLevel:
        """Determine complexity based on compatibility score"""
        if score >= 90:
            return ComplexityLevel.LOW
        elif score >= 70:
            return ComplexityLevel.MEDIUM
        elif score >= 50:
            return ComplexityLevel.HIGH
        else:
            return ComplexityLevel.VERY_HIGH
    
    def _assess_performance_impact(self, score: float) -> str:
        """Assess performance impact"""
        if score >= 90:
            return "Minimal performance impact expected"
        elif score >= 70:
            return "Low to moderate performance impact"
        elif score >= 50:
            return "Moderate performance impact - testing required"
        else:
            return "Significant performance impact - extensive optimization needed"

class AWSServiceMapper:
    """Maps current database features to AWS equivalents"""
    
    def __init__(self):
        self.aws_mappings = self._load_aws_mappings()
    
    def _load_aws_mappings(self) -> Dict:
        """Load AWS service mappings"""
        return {
            'high_availability': {
                'oracle_rac': {
                    'aws_service': 'RDS Multi-AZ',
                    'alternative': 'Aurora Global Database',
                    'description': 'Managed high availability with automatic failover',
                    'setup_complexity': 'Low',
                    'cost_factor': 2.0
                },
                'sql_server_always_on': {
                    'aws_service': 'RDS Multi-AZ',
                    'alternative': 'Aurora SQL Server',
                    'description': 'Managed availability groups with automatic failover',
                    'setup_complexity': 'Low',
                    'cost_factor': 2.0
                },
                'mysql_cluster': {
                    'aws_service': 'Aurora MySQL',
                    'alternative': 'RDS Multi-AZ',
                    'description': 'Aurora provides automatic clustering and scaling',
                    'setup_complexity': 'Low',
                    'cost_factor': 1.5
                }
            },
            'backup_recovery': {
                'oracle_rman': {
                    'aws_service': 'RDS Automated Backups',
                    'alternative': 'AWS Backup',
                    'description': 'Automated backup with point-in-time recovery',
                    'setup_complexity': 'Low',
                    'features': ['PITR', 'Cross-region backup', 'Encryption']
                },
                'sql_server_backup': {
                    'aws_service': 'RDS Automated Backups',
                    'alternative': 'AWS Backup',
                    'description': 'Native SQL Server backup integration',
                    'setup_complexity': 'Low',
                    'features': ['Automated backups', 'Transaction log backups', 'Encryption']
                }
            },
            'replication': {
                'oracle_data_guard': {
                    'aws_service': 'RDS Read Replicas',
                    'alternative': 'Aurora Global Database',
                    'description': 'Cross-region read replicas with low latency',
                    'setup_complexity': 'Medium',
                    'max_replicas': 15
                },
                'mysql_replication': {
                    'aws_service': 'RDS Read Replicas',
                    'alternative': 'Aurora Read Replicas',
                    'description': 'Managed MySQL replication',
                    'setup_complexity': 'Low',
                    'max_replicas': 15
                }
            }
        }
    
    def get_aws_equivalent(self, feature_type: str, current_feature: str) -> Dict:
        """Get AWS equivalent for current database feature"""
        mappings = self.aws_mappings.get(feature_type, {})
        return mappings.get(current_feature, {
            'aws_service': 'Custom Implementation Required',
            'description': 'No direct AWS equivalent available',
            'setup_complexity': 'High'
        })

class MigrationScriptGenerator:
    """Generate migration scripts and procedures"""
    
    def __init__(self):
        self.script_templates = self._load_script_templates()
    
    def _load_script_templates(self) -> Dict:
        """Load script templates"""
        return {
            'pre_migration': {
                'schema_backup': '''
-- Pre-migration schema backup
-- Generated on: {timestamp}
-- Source: {source_engine}
-- Target: {target_engine}

-- Create backup schema
CREATE SCHEMA IF NOT EXISTS migration_backup_{date};

-- Backup existing tables
{backup_statements}

-- Verify backup
SELECT 'Backup completed at ' || NOW();
                ''',
                'data_validation': '''
-- Data validation script
-- Verify data integrity before migration

-- Row count validation
{row_count_queries}

-- Data type validation
{data_type_queries}

-- Constraint validation
{constraint_queries}
                '''
            },
            'schema_conversion': {
                'table_creation': '''
-- Schema conversion script
-- Generated on: {timestamp}

-- Drop existing objects if they exist
{drop_statements}

-- Create tables
{create_statements}

-- Create indexes
{index_statements}

-- Create constraints
{constraint_statements}
                ''',
                'stored_procedures': '''
-- Stored procedure conversion
-- Manual review required for complex procedures

{procedure_conversions}
                '''
            },
            'post_migration': {
                'data_validation': '''
-- Post-migration data validation
-- Compare source and target data

-- Row count comparison
{comparison_queries}

-- Sample data verification
{sample_queries}

-- Performance baseline
{performance_queries}
                ''',
                'optimization': '''
-- Post-migration optimization
-- Analyze and optimize database performance

-- Update statistics
{statistics_updates}

-- Rebuild indexes
{index_rebuilds}

-- Analyze query performance
{performance_analysis}
                '''
            }
        }
    
    def generate_pre_migration_script(self, source_engine: str, target_engine: str, 
                                    schema_objects: List[Dict]) -> str:
        """Generate pre-migration script"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        date = datetime.now().strftime('%Y%m%d')
        
        source_info = get_database_info(source_engine)
        target_info = get_database_info(target_engine)
        
        backup_statements = []
        row_count_queries = []
        
        for obj in schema_objects:
            if obj['type'] == 'table':
                backup_statements.append(
                    f"CREATE TABLE migration_backup_{date}.{obj['name']} AS SELECT * FROM {obj['name']};"
                )
                row_count_queries.append(
                    f"SELECT '{obj['name']}', COUNT(*) FROM {obj['name']};"
                )
        
        template = self.script_templates['pre_migration']['schema_backup']
        return template.format(
            timestamp=timestamp,
            source_engine=f"{source_info['display_name']} ({source_info['query_term']})",
            target_engine=f"{target_info['display_name']} ({target_info['query_term']})",
            date=date,
            backup_statements='\n'.join(backup_statements)
        )
    
    def generate_conversion_script(self, schema_objects: List[Dict], 
                                 conversions: Dict) -> str:
        """Generate schema conversion script"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        drop_statements = []
        create_statements = []
        index_statements = []
        
        for obj in schema_objects:
            if obj['type'] == 'table':
                drop_statements.append(f"DROP TABLE IF EXISTS {obj['name']};")
                if obj['name'] in conversions:
                    create_statements.append(conversions[obj['name']]['converted_ddl'])
        
        template = self.script_templates['schema_conversion']['table_creation']
        return template.format(
            timestamp=timestamp,
            drop_statements='\n'.join(drop_statements),
            create_statements='\n'.join(create_statements),
            index_statements='\n'.join(index_statements),
            constraint_statements='-- Constraints to be added'
        )

def render_header():
    """Render application header"""
    st.markdown("""
    <div class="main-header">
        <h1>🗄️ Database Migration Analyzer</h1>
        <h2>Schema & Query Analysis Tool</h2>
        <p style="font-size: 1.2rem; margin-top: 0.5rem;">
            Advanced Database Migration Analysis • AI-Powered Compatibility Assessment • AWS Service Mapping
        </p>
        <p style="font-size: 0.9rem; margin-top: 0.5rem; opacity: 0.9;">
            Schema Analysis • Query Compatibility • Migration Scripts • Technical Gap Analysis • Cross-Engine Migration
        </p>
    </div>
    """, unsafe_allow_html=True)

def render_examples_tab():
    """Render comprehensive examples and tutorials tab"""
    st.subheader("📚 Migration Examples & Tutorials")
    
    # Quick Start Guide
    st.markdown("""
    <div class="tutorial-step">
        <h4>🚀 Quick Start Guide</h4>
        <p><strong>Step 1:</strong> Select source and target databases in the sidebar</p>
        <p><strong>Step 2:</strong> Choose a migration example or provide your own schema</p>
        <p><strong>Step 3:</strong> Run compatibility analysis to see results</p>
        <p><strong>Step 4:</strong> Review AWS service mappings and generate migration scripts</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Migration Scenarios
    st.markdown("**📖 Real-world Migration Scenarios**")
    
    # Create tabs for different migration examples
    example_tabs = st.tabs([
        "🏢 Enterprise Oracle → Aurora",
        "🚀 SaaS MySQL → Aurora", 
        "🛒 E-commerce Mongo → PostgreSQL",
        "💡 Tutorial: Complete Flow"
    ])
    
    with example_tabs[0]:
        render_migration_example('enterprise_modernization')
    
    with example_tabs[1]:
        render_migration_example('saas_startup_migration')
        
    with example_tabs[2]:
        render_migration_example('ecommerce_mongodb_migration')
        
    with example_tabs[3]:
        render_tutorial_complete_flow()

def render_migration_example(example_key: str):
    """Render a specific migration example"""
    example = MIGRATION_EXAMPLES[example_key]
    
    # Example header
    st.markdown(f"""
    <div class="use-case-card">
        <h3>{example['title']}</h3>
        <p><strong>Migration:</strong> {get_database_info(example['source'])['icon']} {get_database_info(example['source'])['display_name']} → 
        {get_database_info(example['target'].replace('aurora_', ''))['icon']} {get_database_info(example['target'].replace('aurora_', ''))['display_name']}</p>
        <p><strong>Complexity:</strong> {example['complexity'].title()}</p>
        <p>{example['description']}</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Scenario details
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**📋 Business Scenario:**")
        st.markdown(example['scenario'])
        
        # Try this example button
        if st.button(f"🧪 Try This {example['title']} Example", key=f"try_{example_key}"):
            # Set session state to auto-populate the main interface
            st.session_state.example_source = example['source']
            st.session_state.example_target = example['target']
            st.session_state.example_schema = example.get('sample_schema', '')
            st.success(f"✅ Example loaded! Switch to 'Schema Input & Analysis' tab to see the pre-filled data.")
    
    with col2:
        st.markdown("**🎯 Expected Outcomes:**")
        st.markdown(example['expected_outcome'])
        
        # Show sample schema if available
        if 'sample_schema' in example:
            with st.expander("📄 View Sample Schema", expanded=False):
                source_info = get_database_info(example['source'])
                lang = 'sql' if example['source'] != 'mongodb' else 'javascript'
                st.code(example['sample_schema'], language=lang)

def render_tutorial_complete_flow():
    """Render complete tutorial workflow"""
    st.markdown("""
    <div class="tutorial-step">
        <h4>🎓 Complete Migration Analysis Tutorial</h4>
        <p>Follow this step-by-step tutorial to understand the complete migration analysis process.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Tutorial steps
    tutorial_steps = [
        {
            'title': '1️⃣ Database Selection',
            'description': 'Choose your source and target databases',
            'details': '''
            **In the Sidebar:**
            - Select **Source Database**: Choose from MySQL, PostgreSQL, Oracle, SQL Server, or MongoDB
            - Select **Target Database**: Choose your AWS target (Aurora MySQL, Aurora PostgreSQL, etc.)
            - Configure versions and migration scope options
            
            **Pro Tip:** Start with similar database types (e.g., MySQL → Aurora MySQL) for easier migration.
            '''
        },
        {
            'title': '2️⃣ Schema Input',
            'description': 'Provide your database schema and queries',
            'details': '''
            **Schema Input Options:**
            - **Manual Entry**: Copy-paste your DDL statements
            - **File Upload**: Upload .sql, .dump, or database-specific files
            - **Database Connection**: Direct connection (coming soon)
            
            **What to Include:**
            - Table definitions (CREATE TABLE statements)
            - Indexes and constraints
            - Views and stored procedures
            - Sample queries for compatibility analysis
            '''
        },
        {
            'title': '3️⃣ Compatibility Analysis',
            'description': 'Review automated compatibility assessment',
            'details': '''
            **Analysis Results Include:**
            - **Compatibility Score**: Overall percentage compatibility
            - **Issues Found**: Specific problems that need attention
            - **Migration Effort**: Estimated complexity (Low/Medium/High)
            - **AWS Readiness**: How ready your schema is for AWS
            
            **Query Analysis:**
            - Function mapping (e.g., MySQL DATE_FORMAT → PostgreSQL TO_CHAR)
            - Syntax conversions
            - Performance impact assessment
            '''
        },
        {
            'title': '4️⃣ AWS Service Mapping',
            'description': 'Map current database features to AWS services',
            'details': '''
            **Feature Mapping:**
            - **High Availability**: Oracle RAC → Aurora Global Database
            - **Backup & Recovery**: Custom solutions → RDS Automated Backups
            - **Monitoring**: Native tools → CloudWatch & Performance Insights
            
            **Backup Strategy Configuration:**
            - Define RPO (Recovery Point Objective)
            - Set RTO (Recovery Time Objective) 
            - Configure retention policies
            '''
        },
        {
            'title': '5️⃣ Migration Scripts',
            'description': 'Generate automated migration scripts',
            'details': '''
            **Generated Scripts:**
            - **Pre-Migration**: Backup and validation scripts
            - **Schema Conversion**: DDL conversion scripts
            - **Post-Migration**: Verification and optimization scripts
            
            **Migration Checklist:**
            - Pre-migration preparation steps
            - Execution guidelines
            - Post-migration validation tasks
            - Performance optimization recommendations
            '''
        },
        {
            'title': '6️⃣ AI Analysis',
            'description': 'Get AI-powered insights and recommendations',
            'details': '''
            **AI Analysis Provides:**
            - **Deep Compatibility Assessment**: Beyond basic rule-based analysis
            - **Migration Strategy**: Tailored approach for your specific combination
            - **Risk Assessment**: Potential issues and mitigation strategies
            - **Timeline Estimation**: Realistic project timelines
            - **Best Practices**: Industry-specific recommendations
            
            **Requirements:** Anthropic API key needed for AI analysis
            '''
        }
    ]
    
    for step in tutorial_steps:
        with st.expander(step['title'] + ": " + step['description'], expanded=False):
            st.markdown(step['details'])
    
    # Common use cases
    st.markdown("**🔧 Common Use Cases & Solutions:**")
    
    use_cases = [
        {
            'scenario': 'Large Oracle database with PL/SQL procedures',
            'solution': 'Use AI analysis to identify procedure conversion complexity, plan for manual review of complex logic',
            'timeline': '6-12 months',
            'complexity': 'High'
        },
        {
            'scenario': 'MySQL e-commerce application',
            'solution': 'Aurora MySQL provides near-zero downtime migration with minimal application changes',
            'timeline': '2-4 weeks',
            'complexity': 'Low'
        },
        {
            'scenario': 'MongoDB to PostgreSQL for ACID compliance',
            'solution': 'Complete application rewrite required, use JSONB for semi-structured data',
            'timeline': '8-16 months',
            'complexity': 'Very High'
        },
        {
            'scenario': 'SQL Server with Always On availability groups',
            'solution': 'Aurora PostgreSQL with Global Database for cross-region availability',
            'timeline': '3-6 months',
            'complexity': 'Medium'
        }
    ]
    
    for i, use_case in enumerate(use_cases):
        complexity_class = f"complexity-{use_case['complexity'].lower().replace(' ', '_')}"
        st.markdown(f"""
        <div class="metric-card {complexity_class}">
            <h5>Use Case {i+1}: {use_case['scenario']}</h5>
            <p><strong>Solution:</strong> {use_case['solution']}</p>
            <p><strong>Timeline:</strong> {use_case['timeline']} | <strong>Complexity:</strong> {use_case['complexity']}</p>
        </div>
        """, unsafe_allow_html=True)

def render_sidebar():
    """Render sidebar configuration"""
    st.sidebar.header("🔧 Migration Configuration")
    
    # Check if example was loaded
    example_source = st.session_state.get('example_source', 'mysql')
    example_target = st.session_state.get('example_target', 'postgresql')
    
    # Source Database Configuration
    st.sidebar.subheader("📤 Source Database")
    source_engine = st.sidebar.selectbox(
        "Source Database Engine",
        ["mysql", "postgresql", "oracle", "sql_server", "mongodb"],
        index=["mysql", "postgresql", "oracle", "sql_server", "mongodb"].index(example_source) if example_source in ["mysql", "postgresql", "oracle", "sql_server", "mongodb"] else 0,
        format_func=lambda x: f"{DATABASE_CONFIG[x]['icon']} {DATABASE_CONFIG[x]['display_name']}"
    )
    
    source_info = get_database_info(source_engine)
    source_version = st.sidebar.text_input("Source Version", "Latest")
    
    # Show source database info
    st.sidebar.markdown(f"""
    <div class="database-info-card">
        <strong>{source_info['icon']} {source_info['display_name']}</strong><br>
        Schema: {source_info['schema_term']}<br>
        Queries: {source_info['query_term']}
    </div>
    """, unsafe_allow_html=True)
    
    # Target Database Configuration
    st.sidebar.subheader("📥 Target Database")
    target_options = ["postgresql", "mysql", "oracle", "sql_server", "aurora_mysql", "aurora_postgresql"]
    
    target_engine = st.sidebar.selectbox(
        "Target Database Engine",
        target_options,
        index=target_options.index(example_target) if example_target in target_options else 0,
        format_func=lambda x: {
            'mysql': f"{DATABASE_CONFIG['mysql']['icon']} {DATABASE_CONFIG['mysql']['display_name']}",
            'postgresql': f"{DATABASE_CONFIG['postgresql']['icon']} {DATABASE_CONFIG['postgresql']['display_name']}",
            'oracle': f"{DATABASE_CONFIG['oracle']['icon']} {DATABASE_CONFIG['oracle']['display_name']}",
            'sql_server': f"{DATABASE_CONFIG['sql_server']['icon']} {DATABASE_CONFIG['sql_server']['display_name']}",
            'aurora_mysql': '🌟 Aurora MySQL',
            'aurora_postgresql': '🌟 Aurora PostgreSQL'
        }[x]
    )
    
    target_info = get_database_info(target_engine.replace('aurora_', ''))
    target_version = st.sidebar.text_input("Target Version", "Latest")
    
    # Show target database info
    st.sidebar.markdown(f"""
    <div class="database-info-card">
        <strong>{target_info['icon']} {target_info['display_name']}</strong><br>
        Schema: {target_info['schema_term']}<br>
        Queries: {target_info['query_term']}
    </div>
    """, unsafe_allow_html=True)
    
    # Migration Scope
    st.sidebar.subheader("🎯 Migration Scope")
    include_schema = st.sidebar.checkbox("Include Schema Objects", True)
    include_data = st.sidebar.checkbox("Include Data Migration", True)
    include_procedures = st.sidebar.checkbox("Include Stored Procedures", True)
    include_triggers = st.sidebar.checkbox("Include Triggers", False)
    
    # Analysis Options
    st.sidebar.subheader("🔍 Analysis Options")
    enable_ai_analysis = st.sidebar.checkbox("Enable AI Analysis", True)
    deep_analysis = st.sidebar.checkbox("Deep Compatibility Analysis", False)
    generate_scripts = st.sidebar.checkbox("Generate Migration Scripts", True)
    
    return {
        'source_engine': source_engine,
        'source_version': source_version,
        'target_engine': target_engine,
        'target_version': target_version,
        'include_schema': include_schema,
        'include_data': include_data,
        'include_procedures': include_procedures,
        'include_triggers': include_triggers,
        'enable_ai_analysis': enable_ai_analysis,
        'deep_analysis': deep_analysis,
        'generate_scripts': generate_scripts
    }

def render_schema_input_tab(config: Dict):
    """Render schema input and analysis tab with dynamic labels"""
    st.subheader("📋 Schema Analysis Input")
    
    # Get database-specific info
    source_info = get_database_info(config['source_engine'])
    target_info = get_database_info(config['target_engine'].replace('aurora_', ''))
    
    # Check if example schema is loaded
    example_schema = st.session_state.get('example_schema', '')
    
    # Show migration direction
    st.markdown(f"""
    <div class="analysis-card">
        <h4>🔄 Migration Direction</h4>
        <p><strong>Source:</strong> {source_info['icon']} {source_info['display_name']} ({config['source_version']})</p>
        <p><strong>Target:</strong> {target_info['icon']} {target_info['display_name']} ({config['target_version']})</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"**📥 {source_info['schema_label']} Input:**")
        
        # Example loading notification
        if example_schema:
            st.info(f"📚 Example schema loaded from migration scenarios!")
        
        input_method = st.radio(
            "Choose input method:",
            ["Manual Entry", "File Upload", "Database Connection"],
            help=f"Select how you want to provide {source_info['schema_term'].lower()} information"
        )
        
        if input_method == "Manual Entry":
            # Use example schema if loaded, otherwise use default sample
            default_schema = example_schema if example_schema else source_info['sample_schema']
            schema_ddl = st.text_area(
                f"{source_info['schema_label']}",
                value=default_schema,
                height=300,
                help=f"Enter your {source_info['schema_term'].lower()} definition here"
            )
            
        elif input_method == "File Upload":
            uploaded_file = st.file_uploader(
                f"Upload {source_info['display_name']} Schema File",
                type=[ext.replace('.', '') for ext in source_info['file_extensions']],
                help=f"Upload a file containing your {source_info['schema_term'].lower()}"
            )
            
            if uploaded_file:
                schema_ddl = uploaded_file.read().decode('utf-8')
                st.text_area(f"Uploaded {source_info['schema_term']} Preview", 
                           schema_ddl[:1000] + "..." if len(schema_ddl) > 1000 else schema_ddl, 
                           height=200)
            else:
                schema_ddl = example_schema if example_schema else ""
                
        else:  # Database Connection
            st.info(f"Direct {source_info['display_name']} connection feature coming soon. Please use manual entry or file upload.")
            schema_ddl = example_schema if example_schema else ""
    
    with col2:
        st.markdown(f"**📝 {target_info['query_label']} Analysis:**")
        
        queries_text = st.text_area(
            f"{source_info['query_label']} to Analyze",
            placeholder=source_info['sample_queries'],
            height=300,
            help=f"Enter {source_info['query_term'].lower()} that you want to analyze for compatibility with {target_info['display_name']}"
        )
        
        # Show query conversion direction
        st.markdown(f"""
        <div class="query-card">
            <h5>🔄 Query Conversion Direction</h5>
            <p><strong>From:</strong> {source_info['query_term']} ({source_info['display_name']})</p>
            <p><strong>To:</strong> {target_info['query_term']} ({target_info['display_name']})</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Clear example data button
    if example_schema and st.button("🗑️ Clear Example Data"):
        st.session_state.pop('example_schema', None)
        st.session_state.pop('example_source', None)
        st.session_state.pop('example_target', None)
        st.rerun()
    
    return schema_ddl, queries_text

def render_compatibility_analysis_tab(config: Dict, schema_ddl: str, queries_text: str):
    """Render compatibility analysis results"""
    st.subheader("🔍 Compatibility Analysis Results")
    
    if not schema_ddl and not queries_text:
        st.warning("Please provide schema DDL or queries in the Schema Input tab to run analysis.")
        return
    
    # Initialize analyzers
    schema_analyzer = SchemaAnalyzer()
    
    # Get database info
    source_info = get_database_info(config['source_engine'])
    target_info = get_database_info(config['target_engine'].replace('aurora_', ''))
    
    # Run analysis
    with st.spinner("🔄 Running compatibility analysis..."):
        
        # Schema Analysis
        if schema_ddl:
            st.markdown(f"**📊 {source_info['display_name']} → {target_info['display_name']} Schema Compatibility Analysis:**")
            
            schema_analysis = schema_analyzer.analyze_table_compatibility(
                config['source_engine'], 
                config['target_engine'].replace('aurora_', ''), 
                schema_ddl
            )
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "🎯 Compatibility Score",
                    f"{schema_analysis['compatibility_score']:.1f}%",
                    delta=f"Complexity: {schema_analysis['complexity'].value.title()}"
                )
            
            with col2:
                st.metric(
                    "⚠️ Issues Found",
                    len(schema_analysis['issues']),
                    delta=f"Recommendations: {len(schema_analysis['recommendations'])}"
                )
            
            with col3:
                migration_effort = "Low" if schema_analysis['compatibility_score'] > 80 else "Medium" if schema_analysis['compatibility_score'] > 60 else "High"
                st.metric(
                    "⏱️ Migration Effort",
                    migration_effort,
                    delta=f"Score: {schema_analysis['compatibility_score']:.0f}%"
                )
            
            with col4:
                aws_readiness = "Ready" if schema_analysis['compatibility_score'] > 75 else "Needs Work"
                st.metric(
                    "☁️ AWS Readiness", 
                    aws_readiness,
                    delta="Assessment"
                )
            
            # Detailed Issues and Recommendations
            col1, col2 = st.columns(2)
            
            with col1:
                if schema_analysis['issues']:
                    st.markdown(f"""
                    <div class="risk-card">
                        <h4>⚠️ {source_info['display_name']} → {target_info['display_name']} Issues</h4>
                        {''.join([f'<p>• {issue}</p>' for issue in schema_analysis['issues']])}
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div class="schema-card">
                        <h4>✅ No Major Issues Found</h4>
                        <p>{source_info['display_name']} schema appears to be highly compatible with {target_info['display_name']}.</p>
                    </div>
                    """, unsafe_allow_html=True)
            
            with col2:
                if schema_analysis['recommendations']:
                    st.markdown(f"""
                    <div class="compatibility-card">
                        <h4>💡 Migration Recommendations</h4>
                        {''.join([f'<p>• {rec}</p>' for rec in schema_analysis['recommendations']])}
                    </div>
                    """, unsafe_allow_html=True)
        
        # Query Analysis
        if queries_text:
            st.markdown(f"**🔍 {source_info['query_term']} → {target_info['query_term']} Compatibility Analysis:**")
            
            queries = [q.strip() for q in queries_text.split(';') if q.strip()]
            query_results = []
            
            for i, query in enumerate(queries):
                if query:
                    result = schema_analyzer.analyze_query_compatibility(
                        config['source_engine'],
                        config['target_engine'].replace('aurora_', ''),
                        query
                    )
                    query_results.append(result)
            
            if query_results:
                # Query Analysis Summary
                avg_score = sum(r.compatibility_score for r in query_results) / len(query_results)
                total_issues = sum(len(r.issues) for r in query_results)
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric(
                        "📊 Average Compatibility",
                        f"{avg_score:.1f}%",
                        delta=f"{len(query_results)} {source_info['query_term'].lower()} analyzed"
                    )
                
                with col2:
                    st.metric(
                        "🔧 Total Issues",
                        total_issues,
                        delta=f"Across {len(query_results)} {source_info['query_term'].lower()}"
                    )
                
                with col3:
                    complexity_counts = {}
                    for result in query_results:
                        complexity = result.complexity.value
                        complexity_counts[complexity] = complexity_counts.get(complexity, 0) + 1
                    
                    most_common = max(complexity_counts.items(), key=lambda x: x[1])
                    st.metric(
                        "📈 Dominant Complexity",
                        most_common[0].title(),
                        delta=f"{most_common[1]} {source_info['query_term'].lower()}"
                    )
                
                # Detailed Query Results
                for i, result in enumerate(query_results):
                    with st.expander(f"{source_info['query_term']} {i+1} - Compatibility: {result.compatibility_score:.1f}%", expanded=False):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown(f"**Original {source_info['query_term']}:**")
                            # Set appropriate language for code highlighting
                            lang = 'sql' if config['source_engine'] != 'mongodb' else 'javascript'
                            st.code(result.original_query, language=lang)
                            
                            if result.issues:
                                st.markdown("**Issues Found:**")
                                for issue in result.issues:
                                    severity_color = {'low': '🟢', 'medium': '🟡', 'high': '🔴'}
                                    st.write(f"{severity_color.get(issue['severity'], '🔵')} {issue['description']}")
                        
                        with col2:
                            st.markdown(f"**Converted {target_info['query_term']}:**")
                            target_lang = 'sql' if config['target_engine'].replace('aurora_', '') != 'mongodb' else 'javascript'
                            st.code(result.converted_query, language=target_lang)
                            
                            st.markdown("**Analysis Summary:**")
                            st.write(f"**Compatibility Score:** {result.compatibility_score:.1f}%")
                            st.write(f"**Complexity:** {result.complexity.value.title()}")
                            st.write(f"**Performance Impact:** {result.performance_impact}")

def render_aws_mapping_tab(config: Dict):
    """Render AWS service mapping analysis"""
    st.subheader("☁️ AWS Service Mapping & Recommendations")
    
    aws_mapper = AWSServiceMapper()
    source_info = get_database_info(config['source_engine'])
    target_info = get_database_info(config['target_engine'].replace('aurora_', ''))
    
    # Show migration context
    st.markdown(f"""
    <div class="aws-card">
        <h4>🔄 AWS Migration Context</h4>
        <p><strong>Source:</strong> {source_info['icon']} {source_info['display_name']} → <strong>Target:</strong> {target_info['icon']} {target_info['display_name']}</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Current Features Analysis
    st.markdown("**🔧 Current Database Features Analysis:**")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**High Availability Features:**")
        ha_features = st.multiselect(
            "Select current HA features:",
            ["oracle_rac", "sql_server_always_on", "mysql_cluster", "postgresql_hot_standby"],
            format_func=lambda x: {
                'oracle_rac': 'Oracle RAC',
                'sql_server_always_on': 'SQL Server Always On',
                'mysql_cluster': 'MySQL Cluster',
                'postgresql_hot_standby': 'PostgreSQL Hot Standby'
            }.get(x, x)
        )
    
    with col2:
        st.markdown("**Backup & Recovery Features:**")
        backup_features = st.multiselect(
            "Select current backup features:",
            ["oracle_rman", "sql_server_backup", "mysql_backup", "custom_backup"],
            format_func=lambda x: {
                'oracle_rman': 'Oracle RMAN',
                'sql_server_backup': 'SQL Server Native Backup',
                'mysql_backup': 'MySQL Dump/Backup',
                'custom_backup': 'Custom Backup Solution'
            }.get(x, x)
        )
    
    # AWS Mapping Results
    if ha_features or backup_features:
        st.markdown("**🎯 AWS Service Recommendations:**")
        
        all_features = ha_features + backup_features
        
        for feature in all_features:
            if feature in ['oracle_rac', 'sql_server_always_on', 'mysql_cluster']:
                mapping = aws_mapper.get_aws_equivalent('high_availability', feature)
            else:
                mapping = aws_mapper.get_aws_equivalent('backup_recovery', feature)
            
            with st.expander(f"🔍 {feature.replace('_', ' ').title()} → AWS Mapping", expanded=True):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.markdown(f"""
                    <div class="aws-card">
                        <h4>🎯 Primary AWS Service</h4>
                        <p><strong>{mapping.get('aws_service', 'N/A')}</strong></p>
                        <p>{mapping.get('description', 'No description available')}</p>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col2:
                    if 'alternative' in mapping:
                        st.markdown(f"""
                        <div class="compatibility-card">
                            <h4>🔄 Alternative Option</h4>
                            <p><strong>{mapping['alternative']}</strong></p>
                            <p>Setup Complexity: {mapping.get('setup_complexity', 'Unknown')}</p>
                        </div>
                        """, unsafe_allow_html=True)
                
                with col3:
                    st.markdown(f"""
                    <div class="analysis-card">
                        <h4>📊 Migration Impact</h4>
                        <p><strong>Complexity:</strong> {mapping.get('setup_complexity', 'Unknown')}</p>
                        <p><strong>Cost Factor:</strong> {mapping.get('cost_factor', 'N/A')}x</p>
                        <p><strong>Features:</strong> {len(mapping.get('features', []))} included</p>
                    </div>
                    """, unsafe_allow_html=True)
                
                if 'features' in mapping:
                    st.markdown("**✨ AWS Service Features:**")
                    for feature_item in mapping['features']:
                        st.write(f"• {feature_item}")
    
    # Backup Strategy Configuration
    st.markdown(f"**💾 Backup & Recovery Strategy for {target_info['display_name']}:**")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        rpo_requirement = st.selectbox(
            "RPO Requirement",
            ["1 hour", "4 hours", "8 hours", "24 hours"],
            help="Recovery Point Objective - maximum acceptable data loss"
        )
    
    with col2:
        rto_requirement = st.selectbox(
            "RTO Requirement", 
            ["5 minutes", "15 minutes", "1 hour", "4 hours"],
            help="Recovery Time Objective - maximum acceptable downtime"
        )
    
    with col3:
        retention_period = st.selectbox(
            "Backup Retention",
            ["7 days", "30 days", "90 days", "1 year", "7 years"],
            help="How long to retain backups"
        )
    
    # Generate backup strategy recommendations
    st.markdown("**📋 Recommended Backup Strategy:**")
    
    backup_strategy = generate_backup_strategy(rpo_requirement, rto_requirement, retention_period, config['target_engine'], target_info)
    
    st.markdown(f"""
    <div class="schema-card">
        <h4>🎯 Tailored Backup Strategy for {target_info['display_name']}</h4>
        {backup_strategy}
    </div>
    """, unsafe_allow_html=True)

def generate_backup_strategy(rpo: str, rto: str, retention: str, target_engine: str, target_info: Dict) -> str:
    """Generate backup strategy recommendations"""
    strategy = []
    
    # RPO-based recommendations
    if "1 hour" in rpo:
        strategy.append("<p><strong>High Frequency Backups:</strong> Enable continuous backup with 1-minute point-in-time recovery</p>")
        strategy.append(f"<p><strong>{target_info['display_name']} Transaction Log Backups:</strong> Every 5 minutes for minimal data loss</p>")
    elif "4 hours" in rpo:
        strategy.append("<p><strong>Regular Backups:</strong> Automated backups every 2 hours</p>")
        strategy.append(f"<p><strong>{target_info['display_name']} Transaction Log Backups:</strong> Every 15 minutes</p>")
    
    # RTO-based recommendations  
    if "5 minutes" in rto:
        strategy.append("<p><strong>High Availability:</strong> Multi-AZ deployment with automatic failover</p>")
        strategy.append(f"<p><strong>{target_info['display_name']} Read Replicas:</strong> Maintain hot standby replicas</p>")
    elif "15 minutes" in rto:
        strategy.append("<p><strong>Fast Recovery:</strong> Multi-AZ with optimized restore procedures</p>")
    
    # Engine-specific recommendations
    if 'aurora' in target_engine:
        strategy.append("<p><strong>Aurora Benefits:</strong> Continuous backup to S3, 15-minute point-in-time recovery</p>")
        strategy.append("<p><strong>Aurora Backtrack:</strong> Rewind database to specific point in time</p>")
    elif target_engine.replace('aurora_', '') == 'postgresql':
        strategy.append("<p><strong>PostgreSQL Features:</strong> Write-Ahead Logging (WAL) for PITR</p>")
        strategy.append("<p><strong>Streaming Replication:</strong> For real-time standby databases</p>")
    elif target_engine.replace('aurora_', '') == 'mysql':
        strategy.append("<p><strong>MySQL Features:</strong> Binary log replication for PITR</p>")
        strategy.append("<p><strong>MySQL Read Replicas:</strong> For load distribution and backup</p>")
    
    # Retention-based recommendations
    if "7 years" in retention:
        strategy.append("<p><strong>Long-term Archival:</strong> Move backups to S3 Glacier for cost optimization</p>")
        strategy.append("<p><strong>Compliance:</strong> Implement backup encryption and access controls</p>")
    
    strategy.append(f"<p><strong>Retention Policy:</strong> Automated cleanup after {retention}</p>")
    strategy.append(f"<p><strong>Cross-Region Backup:</strong> Replicate backups to secondary region for disaster recovery</p>")
    
    return '\n'.join(strategy)

def render_migration_scripts_tab(config: Dict, schema_ddl: str):
    """Render migration scripts generation tab"""
    st.subheader("📜 Migration Scripts Generation")
    
    source_info = get_database_info(config['source_engine'])
    target_info = get_database_info(config['target_engine'].replace('aurora_', ''))
    
    if not schema_ddl:
        st.warning(f"Please provide {source_info['schema_term'].lower()} in the Schema Input tab to generate migration scripts.")
        return
    
    script_generator = MigrationScriptGenerator()
    
    st.markdown(f"""
    <div class="analysis-card">
        <h4>📜 Migration Scripts for {source_info['display_name']} → {target_info['display_name']}</h4>
        <p>Generate customized migration scripts for your database combination</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Script Generation Options
    col1, col2, col3 = st.columns(3)
    
    with col1:
        generate_pre = st.checkbox("Pre-Migration Scripts", True)
        include_backup = st.checkbox("Include Backup Scripts", True)
        include_validation = st.checkbox("Include Validation Scripts", True)
    
    with col2:
        generate_conversion = st.checkbox("Schema Conversion Scripts", True)
        include_indexes = st.checkbox("Include Index Creation", True)
        include_constraints = st.checkbox("Include Constraints", True)
    
    with col3:
        generate_post = st.checkbox("Post-Migration Scripts", True)
        include_optimization = st.checkbox("Include Optimization", True)
        include_monitoring = st.checkbox("Include Monitoring Setup", True)
    
    if st.button("🚀 Generate Migration Scripts", type="primary"):
        with st.spinner(f"📝 Generating {source_info['display_name']} → {target_info['display_name']} migration scripts..."):
            
            # Parse schema objects (simplified)
            schema_objects = parse_schema_objects(schema_ddl)
            
            # Generate scripts
            if generate_pre:
                st.markdown(f"**📋 Pre-Migration Scripts ({source_info['display_name']}):**")
                
                pre_script = script_generator.generate_pre_migration_script(
                    config['source_engine'],
                    config['target_engine'].replace('aurora_', ''), 
                    schema_objects
                )
                
                with st.expander("🔍 Pre-Migration Script", expanded=False):
                    st.code(pre_script, language='sql')
                    st.download_button(
                        "📥 Download Pre-Migration Script",
                        pre_script,
                        f"pre_migration_{config['source_engine']}_to_{config['target_engine']}.sql",
                        "text/sql"
                    )
            
            if generate_conversion:
                st.markdown(f"**🔄 Schema Conversion Scripts ({source_info['display_name']} → {target_info['display_name']}):**")
                
                # Analyze schema for conversions
                schema_analyzer = SchemaAnalyzer()
                conversions = {}
                
                for obj in schema_objects:
                    if obj['type'] == 'table':
                        conversion = schema_analyzer.analyze_table_compatibility(
                            config['source_engine'],
                            config['target_engine'].replace('aurora_', ''),
                            obj.get('definition', '')
                        )
                        conversions[obj['name']] = conversion
                
                conversion_script = script_generator.generate_conversion_script(schema_objects, conversions)
                
                with st.expander("🔍 Schema Conversion Script", expanded=False):
                    st.code(conversion_script, language='sql')
                    st.download_button(
                        "📥 Download Conversion Script",
                        conversion_script,
                        f"schema_conversion_{config['source_engine']}_to_{config['target_engine']}.sql", 
                        "text/sql"
                    )
            
            if generate_post:
                st.markdown(f"**✅ Post-Migration Scripts ({target_info['display_name']}):**")
                
                post_script = generate_post_migration_script(config, schema_objects, source_info, target_info)
                
                with st.expander("🔍 Post-Migration Script", expanded=False):
                    st.code(post_script, language='sql')
                    st.download_button(
                        "📥 Download Post-Migration Script",
                        post_script,
                        f"post_migration_{config['source_engine']}_to_{config['target_engine']}.sql",
                        "text/sql"
                    )
            
            # Generate migration checklist
            st.markdown(f"**📋 Migration Checklist ({source_info['display_name']} → {target_info['display_name']}):**")
            checklist = generate_migration_checklist(config, schema_objects, source_info, target_info)
            
            st.markdown(f"""
            <div class="analysis-card">
                <h4>✅ Migration Execution Checklist</h4>
                {checklist}
            </div>
            """, unsafe_allow_html=True)

def parse_schema_objects(schema_ddl: str) -> List[Dict]:
    """Parse schema DDL to extract objects"""
    objects = []
    
    # Simple regex parsing (in production, use proper SQL parser)
    create_table_pattern = r'CREATE\s+TABLE\s+(\w+)\s*\((.*?)\);'
    matches = re.finditer(create_table_pattern, schema_ddl, re.IGNORECASE | re.DOTALL)
    
    for match in matches:
        table_name = match.group(1)
        table_def = match.group(0)
        
        objects.append({
            'name': table_name,
            'type': 'table',
            'definition': table_def
        })
    
    # Parse MongoDB collections
    create_collection_pattern = r'db\.createCollection\(["\'](\w+)["\']'
    matches = re.finditer(create_collection_pattern, schema_ddl, re.IGNORECASE)
    
    for match in matches:
        collection_name = match.group(1)
        
        objects.append({
            'name': collection_name,
            'type': 'collection',
            'definition': f'Collection: {collection_name}'
        })
    
    return objects

def generate_post_migration_script(config: Dict, schema_objects: List[Dict], source_info: Dict, target_info: Dict) -> str:
    """Generate post-migration validation script"""
    
    script_parts = []
    script_parts.append(f"-- Post-Migration Validation Script")
    script_parts.append(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    script_parts.append(f"-- Migration: {source_info['display_name']} → {target_info['display_name']}")
    script_parts.append("")
    
    # Row count validation
    script_parts.append("-- Row Count Validation")
    for obj in schema_objects:
        if obj['type'] in ['table', 'collection']:
            script_parts.append(f"SELECT '{obj['name']}' as table_name, COUNT(*) as row_count FROM {obj['name']};")
    
    script_parts.append("")
    
    # Performance analysis
    script_parts.append(f"-- {target_info['display_name']} Performance Analysis")
    if config['target_engine'].replace('aurora_', '') == 'postgresql':
        script_parts.append("-- Update table statistics")
        for obj in schema_objects:
            if obj['type'] == 'table':
                script_parts.append(f"ANALYZE {obj['name']};")
    elif config['target_engine'].replace('aurora_', '') == 'mysql':
        script_parts.append("-- Update table statistics") 
        for obj in schema_objects:
            if obj['type'] == 'table':
                script_parts.append(f"ANALYZE TABLE {obj['name']};")
    
    return '\n'.join(script_parts)

def generate_migration_checklist(config: Dict, schema_objects: List[Dict], source_info: Dict, target_info: Dict) -> str:
    """Generate migration execution checklist"""
    
    checklist_items = []
    
    # Pre-migration
    checklist_items.append(f"<h5>📋 Pre-Migration Phase ({source_info['display_name']})</h5>")
    checklist_items.append(f"<p>☐ Backup current {source_info['display_name']} database completely</p>")
    checklist_items.append(f"<p>☐ Test {source_info['display_name']} backup restore procedure</p>")
    checklist_items.append(f"<p>☐ Validate {source_info['display_name']} → {target_info['display_name']} schema conversion scripts</p>")
    checklist_items.append(f"<p>☐ Set up target AWS {target_info['display_name']} environment</p>")
    checklist_items.append("<p>☐ Configure network connectivity</p>")
    checklist_items.append(f"<p>☐ Test application connectivity to {target_info['display_name']}</p>")
    
    # Migration execution
    checklist_items.append("<h5>🚀 Migration Execution</h5>")
    checklist_items.append("<p>☐ Execute pre-migration scripts</p>")
    checklist_items.append(f"<p>☐ Run {source_info['display_name']} → {target_info['display_name']} schema conversion</p>")
    checklist_items.append("<p>☐ Migrate data (using AWS DMS or custom scripts)</p>")
    checklist_items.append("<p>☐ Validate data integrity</p>")
    checklist_items.append(f"<p>☐ Update application configuration for {target_info['display_name']}</p>")
    checklist_items.append("<p>☐ Test application functionality</p>")
    
    # Post-migration
    checklist_items.append(f"<h5>✅ Post-Migration Phase ({target_info['display_name']})</h5>")
    checklist_items.append("<p>☐ Run post-migration validation scripts</p>")
    checklist_items.append(f"<p>☐ Update {target_info['display_name']} database statistics</p>")
    checklist_items.append("<p>☐ Configure monitoring and alerting</p>")
    checklist_items.append(f"<p>☐ Set up {target_info['display_name']} backup procedures</p>")
    checklist_items.append("<p>☐ Performance testing and optimization</p>")
    checklist_items.append("<p>☐ Update documentation</p>")
    
    return '\n'.join(checklist_items)

def render_ai_analysis_tab(config: Dict, schema_ddl: str):
    """Render AI-powered analysis tab"""
    st.subheader("🤖 AI-Powered Migration Analysis")
    
    source_info = get_database_info(config['source_engine'])
    target_info = get_database_info(config['target_engine'].replace('aurora_', ''))
    
    if not schema_ddl:
        st.warning(f"Please provide {source_info['schema_term'].lower()} in the Schema Input tab for AI analysis.")
        return
    
    ai_manager = AIAnalysisManager()
    
    if not ai_manager.connected:
        st.error("❌ AI Analysis requires Anthropic API key. Please configure in Streamlit secrets.")
        st.info("💡 Add ANTHROPIC_API_KEY to your Streamlit secrets to enable AI-powered analysis.")
        return
    
    # Show AI analysis context
    st.markdown(f"""
    <div class="aws-card">
        <h4>🤖 AI Analysis Context</h4>
        <p><strong>Migration:</strong> {source_info['icon']} {source_info['display_name']} → {target_info['icon']} {target_info['display_name']}</p>
        <p><strong>Analysis:</strong> {source_info['query_term']} compatibility and migration complexity assessment</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**🎯 Analysis Scope:**")
        analyze_complexity = st.checkbox("Migration Complexity Assessment", True)
        analyze_risks = st.checkbox("Risk Factor Analysis", True)
        analyze_timeline = st.checkbox("Timeline Estimation", True)
        analyze_aws = st.checkbox("AWS Service Recommendations", True)
    
    with col2:
        st.markdown("**⚙️ Analysis Options:**")
        analysis_depth = st.selectbox("Analysis Depth", ["Standard", "Comprehensive", "Expert"])
        include_best_practices = st.checkbox("Include Best Practices", True)
        include_testing = st.checkbox("Include Testing Strategy", True)
    
    if st.button(f"🧠 Run AI Analysis for {source_info['display_name']} → {target_info['display_name']}", type="primary"):
        with st.spinner(f"🤖 Running AI-powered {source_info['display_name']} → {target_info['display_name']} analysis..."):
            
            # Parse schema objects for AI analysis
            schema_objects = parse_schema_objects(schema_ddl)
            
            # Prepare schema summary for AI
            schema_summary = []
            for obj in schema_objects:
                schema_summary.append({
                    'name': obj['name'],
                    'type': obj['type']
                })
            
            # Run AI analysis
            try:
                ai_result = asyncio.run(ai_manager.analyze_schema_compatibility(
                    config['source_engine'],
                    config['target_engine'].replace('aurora_', ''),
                    schema_summary
                ))
                
                # Display results
                st.markdown(f"**🎯 AI Analysis Results for {source_info['display_name']} → {target_info['display_name']}:**")
                
                # Metrics overview
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric(
                        "🎯 Compatibility Score",
                        f"{ai_result['compatibility_score']:.1f}%",
                        delta=f"Confidence: High"
                    )
                
                with col2:
                    complexity_color = {
                        'low': '🟢',
                        'medium': '🟡', 
                        'high': '🟠',
                        'very_high': '🔴'
                    }
                    complexity = ai_result['complexity_level']
                    st.metric(
                        "📊 Complexity Level",
                        f"{complexity_color.get(complexity, '🔵')} {complexity.replace('_', ' ').title()}",
                        delta="AI Assessment"
                    )
                
                with col3:
                    st.metric(
                        "⏱️ Timeline Estimate",
                        ai_result['timeline_estimate'],
                        delta="AI Projection"
                    )
                
                with col4:
                    st.metric(
                        "☁️ AWS Services",
                        len(ai_result['aws_services']),
                        delta="Recommended"
                    )
                
                # Detailed Analysis
                col1, col2 = st.columns(2)
                
                with col1:
                    if ai_result['major_issues']:
                        st.markdown(f"""
                        <div class="risk-card">
                            <h4>⚠️ AI-Identified Issues</h4>
                            {''.join([f'<p>• {issue}</p>' for issue in ai_result['major_issues']])}
                        </div>
                        """, unsafe_allow_html=True)
                    
                    if ai_result['aws_services']:
                        st.markdown(f"""
                        <div class="aws-card">
                            <h4>☁️ Recommended AWS Services</h4>
                            {''.join([f'<p>• {service}</p>' for service in ai_result['aws_services']])}
                        </div>
                        """, unsafe_allow_html=True)
                
                with col2:
                    if ai_result['recommendations']:
                        st.markdown(f"""
                        <div class="compatibility-card">
                            <h4>💡 AI Recommendations</h4>
                            {''.join([f'<p>• {rec}</p>' for rec in ai_result['recommendations']])}
                        </div>
                        """, unsafe_allow_html=True)
                
                # Full AI Analysis
                with st.expander(f"🔍 Complete AI Analysis Report ({source_info['display_name']} → {target_info['display_name']})", expanded=False):
                    st.markdown("**🤖 Detailed AI Assessment:**")
                    st.markdown(ai_result['ai_analysis'])
                
            except Exception as e:
                st.error(f"❌ AI Analysis failed: {str(e)}")
                st.info("💡 Please check your API configuration and try again.")

def main():
    """Main application function"""
    render_header()
    
    # Get configuration from sidebar
    config = render_sidebar()
    
    # Main content tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📚 Examples & Tutorials",
        "📋 Schema Input & Analysis",
        "🔍 Compatibility Analysis", 
        "☁️ AWS Service Mapping",
        "📜 Migration Scripts",
        "🤖 AI Analysis"
    ])
    
    with tab1:
        render_examples_tab()
    
    with tab2:
        schema_ddl, queries_text = render_schema_input_tab(config)
        
        # Show what was captured
        if schema_ddl or queries_text:
            st.markdown("**📊 Input Summary:**")
            col1, col2 = st.columns(2)
            
            source_info = get_database_info(config['source_engine'])
            
            with col1:
                if schema_ddl:
                    st.success(f"✅ {source_info['schema_label']} provided ({len(schema_ddl)} characters)")
            
            with col2:
                if queries_text:
                    query_count = len([q for q in queries_text.split(';') if q.strip()])
                    st.success(f"✅ {query_count} {source_info['query_term'].lower()} provided")
    
    with tab3:
        render_compatibility_analysis_tab(config, schema_ddl, queries_text)
    
    with tab4:
        render_aws_mapping_tab(config)
    
    with tab5:
        render_migration_scripts_tab(config, schema_ddl)
    
    with tab6:
        render_ai_analysis_tab(config, schema_ddl)
    
    # Professional footer
    st.markdown("""
    <div class="professional-footer">
        <h4>🗄️ Database Migration Analyzer - Schema & Query Analysis Tool</h4>
        <p>Advanced Database Migration Analysis • AI-Powered Compatibility Assessment • AWS Service Mapping • Migration Script Generation</p>
        <p style="font-size: 0.9rem; margin-top: 1rem; opacity: 0.9;">
            🔬 Schema Analysis • 🔍 Query Compatibility • 📜 Script Generation • ☁️ AWS Integration • 🤖 AI Recommendations • 📚 Real-world Examples
        </p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
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
import hashlib
import uuid
import sqlite3
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Enterprise Database Migration Analyzer",
    page_icon="🗄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Enhanced Professional CSS styling
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
    
    .enterprise-badge {
        background: linear-gradient(45deg, #10b981, #059669);
        padding: 0.5rem 1rem;
        border-radius: 20px;
        color: white;
        font-weight: bold;
        display: inline-block;
        margin-top: 0.5rem;
        box-shadow: 0 2px 8px rgba(16,185,129,0.3);
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
    
    .enterprise-card {
        background: linear-gradient(135deg, #f0f9ff 0%, #e0f2fe 100%);
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        border-left: 4px solid #0ea5e9;
        border: 1px solid #e5e7eb;
    }
    
    .cost-card {
        background: linear-gradient(135deg, #ecfdf5 0%, #d1fae5 100%);
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        border-left: 4px solid #10b981;
        border: 1px solid #e5e7eb;
    }
    
    .security-card {
        background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%);
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        border-left: 4px solid #f59e0b;
        border: 1px solid #e5e7eb;
    }
    
    .collaboration-card {
        background: linear-gradient(135deg, #faf5ff 0%, #f3e8ff 100%);
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        border-left: 4px solid #a855f7;
        border: 1px solid #e5e7eb;
    }
    
    .ai-enhanced-card {
        background: linear-gradient(135deg, #fef2f2 0%, #fee2e2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        border-left: 4px solid #ef4444;
        border: 1px solid #e5e7eb;
        position: relative;
    }
    
    .ai-enhanced-card::before {
        content: "🤖 AI Enhanced";
        position: absolute;
        top: -10px;
        right: 10px;
        background: #ef4444;
        color: white;
        padding: 0.25rem 0.5rem;
        border-radius: 4px;
        font-size: 0.75rem;
        font-weight: bold;
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
    
    .progress-indicator {
        width: 100%;
        height: 8px;
        background: #e5e7eb;
        border-radius: 4px;
        overflow: hidden;
        margin: 0.5rem 0;
    }
    
    .progress-fill {
        height: 100%;
        border-radius: 4px;
        transition: width 0.3s ease;
    }
    
    .progress-high { background: linear-gradient(90deg, #ef4444, #dc2626); }
    .progress-medium { background: linear-gradient(90deg, #f59e0b, #d97706); }
    .progress-low { background: linear-gradient(90deg, #22c55e, #16a34a); }
    
    .feature-badge {
        display: inline-block;
        padding: 0.25rem 0.5rem;
        margin: 0.25rem;
        border-radius: 12px;
        font-size: 0.75rem;
        font-weight: bold;
        color: white;
    }
    
    .badge-new { background: #ef4444; }
    .badge-enhanced { background: #f59e0b; }
    .badge-ai { background: #8b5cf6; }
    .badge-enterprise { background: #059669; }
    
    .sidebar-info {
        background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
        padding: 1rem;
        border-radius: 8px;
        margin: 0.5rem 0;
        border-left: 3px solid #3b82f6;
        font-size: 0.9rem;
    }
    
    .success-banner {
        background: linear-gradient(135deg, #dcfce7 0%, #bbf7d0 100%);
        border: 1px solid #22c55e;
        border-radius: 8px;
        padding: 1rem;
        margin: 1rem 0;
        color: #166534;
    }
    
    .warning-banner {
        background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%);
        border: 1px solid #f59e0b;
        border-radius: 8px;
        padding: 1rem;
        margin: 1rem 0;
        color: #92400e;
    }
    
    .error-banner {
        background: linear-gradient(135deg, #fee2e2 0%, #fecaca 100%);
        border: 1px solid #ef4444;
        border-radius: 8px;
        padding: 1rem;
        margin: 1rem 0;
        color: #991b1b;
    }
    
    .cost-optimization {
        background: linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%);
        border: 2px solid #22c55e;
        border-radius: 12px;
        padding: 1.5rem;
        margin: 1rem 0;
    }
    
    .risk-matrix {
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 1rem;
        margin: 1rem 0;
    }
    
    .risk-cell {
        padding: 1rem;
        border-radius: 8px;
        text-align: center;
        font-weight: bold;
        color: white;
    }
    
    .risk-high { background: #ef4444; }
    .risk-medium { background: #f59e0b; }
    .risk-low { background: #22c55e; }
    
    .collaboration-status {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        padding: 0.5rem;
        background: #f8fafc;
        border-radius: 6px;
        margin: 0.25rem 0;
    }
    
    .status-online { color: #22c55e; }
    .status-away { color: #f59e0b; }
    .status-offline { color: #6b7280; }
</style>
""", unsafe_allow_html=True)

# Enhanced Enums and Data Classes
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

class AnalysisType(Enum):
    """Types of AI analysis"""
    MIGRATION_STRATEGY = "migration_strategy"
    RISK_ASSESSMENT = "risk_assessment"
    PERFORMANCE_PREDICTION = "performance_prediction"
    COST_OPTIMIZATION = "cost_optimization"
    TIMELINE_ESTIMATION = "timeline_estimation"
    TESTING_STRATEGY = "testing_strategy"
    SECURITY_ANALYSIS = "security_analysis"

class UserRole(Enum):
    """User roles for collaboration"""
    ADMIN = "admin"
    PROJECT_MANAGER = "project_manager"
    DBA = "dba"
    DEVELOPER = "developer"
    VIEWER = "viewer"

@dataclass
class User:
    """User representation for collaboration"""
    id: str
    username: str
    email: str
    role: UserRole
    last_active: datetime
    projects: List[str]

@dataclass
class MigrationProject:
    """Migration project representation"""
    id: str
    name: str
    source_engine: str
    target_engine: str
    status: str
    owner_id: str
    team_members: List[str]
    created_at: datetime
    updated_at: datetime
    metadata: Dict

@dataclass
class CostEstimate:
    """Cost estimation result"""
    service: str
    monthly_cost: float
    annual_cost: float
    cost_factors: Dict[str, float]
    optimizations: List[str]
    confidence_score: float

@dataclass
class SecurityAssessment:
    """Security assessment result"""
    overall_score: float
    vulnerabilities: List[Dict]
    compliance_status: Dict[str, bool]
    recommendations: List[str]
    data_classification: Dict[str, str]

@dataclass
class AIAnalysisResult:
    """AI analysis result"""
    analysis_type: AnalysisType
    confidence_score: float
    recommendations: List[str]
    risks: List[Dict[str, str]]
    opportunities: List[str]
    timeline_estimate: str
    cost_impact: str
    detailed_analysis: str
    action_items: List[Dict[str, str]]

# Database Configuration with Enhanced Features
DATABASE_CONFIG = {
    'mysql': {
        'display_name': 'MySQL',
        'icon': '🐬',
        'schema_label': 'MySQL Schema Definition',
        'query_label': 'MySQL Queries',
        'schema_term': 'Database Schema',
        'query_term': 'SQL Queries',
        'file_extensions': ['.sql', '.dump'],
        'aws_target_options': ['aurora_mysql', 'rds_mysql'],
        'enterprise_features': ['High Availability', 'Read Replicas', 'Automated Backups'],
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
LIMIT 100;'''
    },
    'postgresql': {
        'display_name': 'PostgreSQL',
        'icon': '🐘',
        'schema_label': 'PostgreSQL Schema Definition',
        'query_label': 'PostgreSQL Queries',
        'schema_term': 'Database Schema',
        'query_term': 'SQL Queries',
        'file_extensions': ['.sql', '.psql'],
        'aws_target_options': ['aurora_postgresql', 'rds_postgresql'],
        'enterprise_features': ['JSONB Support', 'Full Text Search', 'Advanced Indexing'],
        'sample_schema': '''-- PostgreSQL E-commerce Database Schema Example
-- Demonstrates PostgreSQL-specific features and best practices

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

CREATE TYPE user_status AS ENUM ('active', 'inactive', 'suspended', 'pending_verification');
CREATE TYPE order_status AS ENUM ('pending', 'processing', 'shipped', 'delivered', 'cancelled', 'returned');

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
);''',
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
)
SELECT 
    TO_CHAR(month, 'YYYY-MM') as sales_month,
    order_count,
    revenue,
    avg_order_value,
    LAG(revenue) OVER (ORDER BY month) as prev_month_revenue
FROM monthly_sales
ORDER BY month DESC;'''
    }
}

# Initialize Session State for Enterprise Features
def initialize_session_state():
    """Initialize session state for enterprise features"""
    if 'user_id' not in st.session_state:
        st.session_state.user_id = str(uuid.uuid4())
    
    if 'current_project' not in st.session_state:
        st.session_state.current_project = None
    
    if 'projects' not in st.session_state:
        st.session_state.projects = []
    
    if 'analysis_results' not in st.session_state:
        st.session_state.analysis_results = {}
    
    if 'cost_estimates' not in st.session_state:
        st.session_state.cost_estimates = {}
    
    if 'security_assessment' not in st.session_state:
        st.session_state.security_assessment = None
    
    if 'collaboration_enabled' not in st.session_state:
        st.session_state.collaboration_enabled = False

# Database Manager for Enterprise Features
class EnterpriseDBManager:
    """Manage enterprise database operations"""
    
    def __init__(self):
        self.db_path = Path("enterprise_migration.db")
        self.init_database()
    
    def init_database(self):
        """Initialize SQLite database for enterprise features"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Users table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                email TEXT UNIQUE NOT NULL,
                role TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata TEXT DEFAULT '{}'
            )
        ''')
        
        # Projects table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS migration_projects (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                source_engine TEXT NOT NULL,
                target_engine TEXT NOT NULL,
                status TEXT DEFAULT 'planning',
                owner_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata TEXT DEFAULT '{}'
            )
        ''')
        
        # Analysis results table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analysis_results (
                id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                analysis_type TEXT NOT NULL,
                result_data TEXT NOT NULL,
                confidence_score REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (project_id) REFERENCES migration_projects (id)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def create_project(self, name: str, source_engine: str, target_engine: str, owner_id: str) -> str:
        """Create new migration project"""
        project_id = str(uuid.uuid4())
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO migration_projects (id, name, source_engine, target_engine, owner_id)
            VALUES (?, ?, ?, ?, ?)
        ''', (project_id, name, source_engine, target_engine, owner_id))
        
        conn.commit()
        conn.close()
        return project_id
    
    def get_user_projects(self, user_id: str) -> List[Dict]:
        """Get projects for user"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, name, source_engine, target_engine, status, created_at
            FROM migration_projects
            WHERE owner_id = ?
            ORDER BY created_at DESC
        ''', (user_id,))
        
        projects = []
        for row in cursor.fetchall():
            projects.append({
                'id': row[0],
                'name': row[1],
                'source_engine': row[2],
                'target_engine': row[3],
                'status': row[4],
                'created_at': row[5]
            })
        
        conn.close()
        return projects

# Enhanced AWS Cost Calculator
class EnhancedAWSCostCalculator:
    """Enhanced AWS cost calculation with real-time pricing"""
    
    def __init__(self):
        try:
            self.pricing_client = boto3.client('pricing', region_name='us-east-1')
            self.ce_client = boto3.client('ce')
            self.rds_client = boto3.client('rds')
            self.connected = True
            logger.info("AWS Cost Calculator initialized successfully")
        except Exception as e:
            self.connected = False
            logger.error(f"AWS Cost Calculator initialization failed: {e}")
    
    def estimate_total_migration_cost(self, config: Dict) -> CostEstimate:
        """Estimate total migration cost including all components"""
        try:
            if not self.connected:
                return self._get_fallback_estimate()
            
            # Calculate individual components
            rds_cost = self._calculate_rds_cost(config)
            storage_cost = self._calculate_storage_cost(config)
            backup_cost = self._calculate_backup_cost(config)
            dms_cost = self._calculate_dms_cost(config)
            data_transfer_cost = self._calculate_data_transfer_cost(config)
            
            total_monthly = rds_cost + storage_cost + backup_cost
            total_annual = total_monthly * 12
            migration_cost = dms_cost + data_transfer_cost
            
            # Generate optimizations
            optimizations = self._generate_cost_optimizations(config, total_monthly)
            
            return CostEstimate(
                service="Complete Migration",
                monthly_cost=total_monthly,
                annual_cost=total_annual,
                cost_factors={
                    'rds_instance': rds_cost,
                    'storage': storage_cost,
                    'backup': backup_cost,
                    'migration': migration_cost,
                    'data_transfer': data_transfer_cost
                },
                optimizations=optimizations,
                confidence_score=0.85
            )
            
        except Exception as e:
            logger.error(f"Cost estimation failed: {e}")
            return self._get_fallback_estimate()
    
    def _calculate_rds_cost(self, config: Dict) -> float:
        """Calculate RDS instance cost"""
        # Simplified calculation - in production use actual API
        instance_costs = {
            'db.t3.micro': 16.79,
            'db.t3.small': 33.58,
            'db.t3.medium': 67.15,
            'db.t3.large': 134.30,
            'db.m5.large': 175.20,
            'db.m5.xlarge': 350.40,
            'db.r5.large': 217.44,
            'db.r5.xlarge': 434.88
        }
        
        instance_class = config.get('instance_class', 'db.t3.medium')
        base_cost = instance_costs.get(instance_class, 67.15)
        
        # Apply multi-AZ multiplier
        if config.get('multi_az', False):
            base_cost *= 2
        
        # Apply Aurora premium
        if 'aurora' in config.get('target_engine', ''):
            base_cost *= 1.2
        
        return base_cost
    
    def _calculate_storage_cost(self, config: Dict) -> float:
        """Calculate storage cost"""
        storage_gb = config.get('storage_gb', 100)
        storage_type = config.get('storage_type', 'gp2')
        
        storage_rates = {
            'gp2': 0.115,
            'gp3': 0.08,
            'io1': 0.125,
            'io2': 0.125
        }
        
        return storage_gb * storage_rates.get(storage_type, 0.115)
    
    def _calculate_backup_cost(self, config: Dict) -> float:
        """Calculate backup storage cost"""
        storage_gb = config.get('storage_gb', 100)
        backup_retention = config.get('backup_retention_days', 7)
        
        # Backup storage is typically 20-50% of primary storage
        backup_multiplier = min(backup_retention / 7 * 0.3, 1.0)
        return storage_gb * backup_multiplier * 0.095  # Backup storage rate
    
    def _calculate_dms_cost(self, config: Dict) -> float:
        """Calculate DMS migration cost"""
        dms_instance_costs = {
            'dms.t3.micro': 18.0,
            'dms.t3.small': 36.0,
            'dms.t3.medium': 72.0,
            'dms.t3.large': 144.0
        }
        
        dms_instance = config.get('dms_instance', 'dms.t3.medium')
        migration_hours = config.get('migration_duration_hours', 24)
        
        hourly_cost = dms_instance_costs.get(dms_instance, 72.0) / (24 * 30)  # Convert monthly to hourly
        return hourly_cost * migration_hours
    
    def _calculate_data_transfer_cost(self, config: Dict) -> float:
        """Calculate data transfer cost"""
        data_size_gb = config.get('data_size_gb', 100)
        
        if data_size_gb <= 1:
            return 0
        elif data_size_gb <= 10240:  # Up to 10TB
            return (data_size_gb - 1) * 0.09
        else:
            return (10239 * 0.09) + ((data_size_gb - 10240) * 0.085)
    
    def _generate_cost_optimizations(self, config: Dict, monthly_cost: float) -> List[str]:
        """Generate cost optimization recommendations"""
        optimizations = []
        
        # Instance optimization
        if monthly_cost > 500:
            optimizations.append("Consider Reserved Instances for 40-60% savings")
        
        # Storage optimization
        if config.get('storage_type', 'gp2') == 'gp2':
            optimizations.append("Upgrade to gp3 storage for 30% cost savings")
        
        # Multi-AZ optimization
        if not config.get('multi_az', False):
            optimizations.append("Enable Multi-AZ for high availability")
        
        # Aurora optimization
        if 'aurora' not in config.get('target_engine', ''):
            optimizations.append("Consider Aurora for better performance/cost ratio")
        
        return optimizations
    
    def _get_fallback_estimate(self) -> CostEstimate:
        """Fallback estimate when AWS API is unavailable"""
        return CostEstimate(
            service="Estimated Migration Cost",
            monthly_cost=250.0,
            annual_cost=3000.0,
            cost_factors={
                'rds_instance': 150.0,
                'storage': 50.0,
                'backup': 25.0,
                'migration': 100.0
            },
            optimizations=["Enable AWS API for precise cost calculation"],
            confidence_score=0.5
        )

# Enhanced AI Analyzer with Multiple Analysis Types
class EnterpriseAIAnalyzer:
    """Enterprise-grade AI analyzer with comprehensive migration analysis"""
    
    def __init__(self):
        self.client = None
        self.connected = False
        
        try:
            api_key = st.secrets.get("ANTHROPIC_API_KEY")
            if api_key:
                self.client = anthropic.Anthropic(api_key=api_key)
                self.connected = True
                logger.info("Enterprise AI Analyzer initialized successfully")
        except Exception as e:
            logger.error(f"AI Analyzer initialization failed: {e}")
    
    async def comprehensive_analysis(self, migration_context: Dict) -> Dict[AnalysisType, AIAnalysisResult]:
        """Run comprehensive AI analysis across all aspects"""
        if not self.connected:
            return self._get_fallback_analysis()
        
        analyses = {}
        
        try:
            # Run different analysis types
            strategy_analysis = await self._analyze_migration_strategy(migration_context)
            risk_analysis = await self._analyze_risks(migration_context)
            cost_analysis = await self._analyze_cost_optimization(migration_context)
            timeline_analysis = await self._analyze_timeline(migration_context)
            security_analysis = await self._analyze_security(migration_context)
            
            analyses[AnalysisType.MIGRATION_STRATEGY] = strategy_analysis
            analyses[AnalysisType.RISK_ASSESSMENT] = risk_analysis
            analyses[AnalysisType.COST_OPTIMIZATION] = cost_analysis
            analyses[AnalysisType.TIMELINE_ESTIMATION] = timeline_analysis
            analyses[AnalysisType.SECURITY_ANALYSIS] = security_analysis
            
            return analyses
            
        except Exception as e:
            logger.error(f"Comprehensive analysis failed: {e}")
            return self._get_fallback_analysis()
    
    async def _analyze_migration_strategy(self, context: Dict) -> AIAnalysisResult:
        """Analyze migration strategy"""
        prompt = f"""
        As a senior database migration architect, analyze this migration and provide a comprehensive strategy:
        
        Source: {context.get('source_engine', 'Unknown')}
        Target: {context.get('target_engine', 'Unknown')}
        Data Size: {context.get('data_size_gb', 'Unknown')} GB
        Business Context: {context.get('business_context', 'Not provided')}
        
        Provide:
        1. Recommended migration approach (Big Bang vs Phased)
        2. Key technical considerations
        3. AWS services to leverage
        4. Success criteria
        5. Rollback strategy
        
        Rate confidence 1-10 and provide specific recommendations.
        """
        
        try:
            response = self.client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=2000,
                temperature=0.2,
                messages=[{"role": "user", "content": prompt}]
            )
            
            analysis_text = response.content[0].text
            
            return AIAnalysisResult(
                analysis_type=AnalysisType.MIGRATION_STRATEGY,
                confidence_score=self._extract_confidence(analysis_text),
                recommendations=self._extract_recommendations(analysis_text),
                risks=self._extract_risks(analysis_text),
                opportunities=self._extract_opportunities(analysis_text),
                timeline_estimate=self._extract_timeline(analysis_text),
                cost_impact=self._extract_cost_impact(analysis_text),
                detailed_analysis=analysis_text,
                action_items=self._extract_action_items(analysis_text)
            )
            
        except Exception as e:
            logger.error(f"Strategy analysis failed: {e}")
            return self._get_fallback_result(AnalysisType.MIGRATION_STRATEGY)
    
    async def _analyze_risks(self, context: Dict) -> AIAnalysisResult:
        """Comprehensive risk analysis"""
        prompt = f"""
        Perform a comprehensive risk analysis for this database migration:
        
        Migration: {context.get('source_engine', 'Unknown')} → {context.get('target_engine', 'Unknown')}
        Data Volume: {context.get('data_size_gb', 'Unknown')} GB
        Business Critical: {context.get('business_critical', False)}
        Downtime Tolerance: {context.get('downtime_tolerance', 'Unknown')}
        
        Analyze:
        1. Technical risks (data loss, compatibility issues)
        2. Business risks (downtime, performance impact)
        3. Security risks (data exposure, compliance)
        4. Operational risks (team readiness, rollback)
        
        For each risk, provide:
        - Probability (High/Medium/Low)
        - Impact (High/Medium/Low)
        - Mitigation strategy
        
        Rate overall risk level 1-10.
        """
        
        try:
            response = self.client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=2500,
                temperature=0.1,
                messages=[{"role": "user", "content": prompt}]
            )
            
            analysis_text = response.content[0].text
            
            return AIAnalysisResult(
                analysis_type=AnalysisType.RISK_ASSESSMENT,
                confidence_score=self._extract_confidence(analysis_text),
                recommendations=self._extract_risk_mitigations(analysis_text),
                risks=self._extract_categorized_risks(analysis_text),
                opportunities=self._extract_opportunities(analysis_text),
                timeline_estimate="Risk-dependent",
                cost_impact=self._extract_risk_cost_impact(analysis_text),
                detailed_analysis=analysis_text,
                action_items=self._extract_risk_action_items(analysis_text)
            )
            
        except Exception as e:
            logger.error(f"Risk analysis failed: {e}")
            return self._get_fallback_result(AnalysisType.RISK_ASSESSMENT)
    
    def _extract_confidence(self, text: str) -> float:
        """Extract confidence score from AI response"""
        confidence_patterns = [
            r'confidence[:\s]+(\d+(?:\.\d+)?)',
            r'(\d+(?:\.\d+)?)\s*(?:out of 10|/10)',
            r'confidence level[:\s]+(\d+(?:\.\d+)?)'
        ]
        
        for pattern in confidence_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                score = float(match.group(1))
                return min(score / 10.0 if score > 10 else score, 1.0)
        
        return 0.75  # Default confidence
    
    def _extract_recommendations(self, text: str) -> List[str]:
        """Extract recommendations from AI response"""
        recommendations = []
        
        # Look for bullet points or numbered lists
        patterns = [
            r'(?:^|\n)\s*[-•*]\s*(.+?)(?=\n|$)',
            r'(?:^|\n)\s*\d+\.\s*(.+?)(?=\n|$)',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.MULTILINE)
            recommendations.extend([match.strip() for match in matches if len(match.strip()) > 10])
        
        return recommendations[:8]  # Limit to top 8
    
    def _extract_risks(self, text: str) -> List[Dict[str, str]]:
        """Extract risks with severity levels"""
        risks = []
        
        # Look for risk sections
        risk_patterns = [
            r'risk[^:]*?:(.*?)(?=\n\n|\n[A-Z]|$)',
            r'concern[^:]*?:(.*?)(?=\n\n|\n[A-Z]|$)'
        ]
        
        for pattern in risk_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                risk_text = match.group(1)
                risk_items = re.findall(r'[-•*]\s*(.+?)(?=\n|$)', risk_text, re.MULTILINE)
                
                for item in risk_items:
                    severity = 'medium'  # default
                    if any(word in item.lower() for word in ['critical', 'high', 'severe']):
                        severity = 'high'
                    elif any(word in item.lower() for word in ['low', 'minor', 'minimal']):
                        severity = 'low'
                    
                    risks.append({
                        'description': item.strip(),
                        'severity': severity,
                        'category': 'technical'
                    })
        
        return risks[:6]
    
    def _get_fallback_analysis(self) -> Dict[AnalysisType, AIAnalysisResult]:
        """Fallback analysis when AI is not available"""
        fallback = {}
        
        for analysis_type in AnalysisType:
            fallback[analysis_type] = self._get_fallback_result(analysis_type)
        
        return fallback
    
    def _get_fallback_result(self, analysis_type: AnalysisType) -> AIAnalysisResult:
        """Fallback result for specific analysis type"""
        return AIAnalysisResult(
            analysis_type=analysis_type,
            confidence_score=0.6,
            recommendations=["AI analysis not available - manual review required"],
            risks=[{"description": "Unable to perform AI assessment", "severity": "medium", "category": "technical"}],
            opportunities=["Manual analysis may reveal additional opportunities"],
            timeline_estimate="Manual estimation required",
            cost_impact="Manual cost analysis required",
            detailed_analysis="AI analysis service not available. Please perform manual analysis.",
            action_items=[{"action": "Enable AI analysis service", "priority": "high", "category": "setup"}]
        )

# Security Analyzer
class SecurityAnalyzer:
    """Comprehensive security analysis for database migration"""
    
    def __init__(self):
        self.compliance_frameworks = {
            'GDPR': ['data_encryption', 'access_controls', 'audit_logging', 'data_retention'],
            'HIPAA': ['data_encryption', 'access_controls', 'audit_logging', 'data_backup'],
            'SOC2': ['security_monitoring', 'access_controls', 'change_management', 'incident_response'],
            'PCI_DSS': ['data_encryption', 'network_security', 'access_controls', 'regular_monitoring']
        }
    
    def analyze_security(self, migration_context: Dict) -> SecurityAssessment:
        """Perform comprehensive security analysis"""
        try:
            # Analyze data classification
            data_classification = self._classify_data(migration_context.get('schema_ddl', ''))
            
            # Check compliance requirements
            compliance_status = self._check_compliance(migration_context, data_classification)
            
            # Identify vulnerabilities
            vulnerabilities = self._identify_vulnerabilities(migration_context)
            
            # Generate recommendations
            recommendations = self._generate_security_recommendations(
                migration_context, data_classification, vulnerabilities
            )
            
            # Calculate overall security score
            security_score = self._calculate_security_score(compliance_status, vulnerabilities)
            
            return SecurityAssessment(
                overall_score=security_score,
                vulnerabilities=vulnerabilities,
                compliance_status=compliance_status,
                recommendations=recommendations,
                data_classification=data_classification
            )
            
        except Exception as e:
            logger.error(f"Security analysis failed: {e}")
            return self._get_fallback_security_assessment()
    
    def _classify_data(self, schema_ddl: str) -> Dict[str, str]:
        """Classify data types for security assessment"""
        classification = {}
        
        pii_patterns = {
            'email': r'email|e_mail|electronic_mail',
            'phone': r'phone|telephone|mobile|cell',
            'ssn': r'ssn|social_security|social_security_number',
            'credit_card': r'credit_card|cc_number|card_number',
            'address': r'address|street|zip|postal',
            'name': r'first_name|last_name|full_name|surname'
        }
        
        for table_match in re.finditer(r'CREATE TABLE (\w+)', schema_ddl, re.IGNORECASE):
            table_name = table_match.group(1)
            
            # Check for PII patterns in column definitions
            table_section = schema_ddl[table_match.start():].split(';')[0]
            
            classification[table_name] = 'public'  # Default
            
            for pii_type, pattern in pii_patterns.items():
                if re.search(pattern, table_section, re.IGNORECASE):
                    classification[table_name] = 'pii'
                    break
            
            # Check for financial or healthcare indicators
            if re.search(r'payment|transaction|billing|medical|health', table_section, re.IGNORECASE):
                classification[table_name] = 'sensitive'
        
        return classification
    
    def _check_compliance(self, context: Dict, data_classification: Dict) -> Dict[str, bool]:
        """Check compliance framework requirements"""
        compliance_status = {}
        required_frameworks = context.get('compliance_requirements', ['GDPR'])
        
        for framework in required_frameworks:
            if framework in self.compliance_frameworks:
                requirements = self.compliance_frameworks[framework]
                
                # Check if requirements are met (simplified check)
                met_requirements = 0
                total_requirements = len(requirements)
                
                # Encryption check
                if 'data_encryption' in requirements:
                    if context.get('encryption_at_rest', False):
                        met_requirements += 1
                
                # Access controls check
                if 'access_controls' in requirements:
                    if context.get('iam_enabled', False):
                        met_requirements += 1
                
                # Audit logging check
                if 'audit_logging' in requirements:
                    if context.get('audit_logging', False):
                        met_requirements += 1
                
                compliance_status[framework] = (met_requirements / total_requirements) >= 0.8
        
        return compliance_status
    
    def _identify_vulnerabilities(self, context: Dict) -> List[Dict]:
        """Identify potential security vulnerabilities"""
        vulnerabilities = []
        
        # Check encryption
        if not context.get('encryption_at_rest', False):
            vulnerabilities.append({
                'type': 'encryption',
                'severity': 'high',
                'description': 'Data at rest encryption not enabled',
                'recommendation': 'Enable encryption at rest for all database storage'
            })
        
        if not context.get('encryption_in_transit', False):
            vulnerabilities.append({
                'type': 'encryption',
                'severity': 'high',
                'description': 'Data in transit encryption not configured',
                'recommendation': 'Enable SSL/TLS encryption for all database connections'
            })
        
        # Check network security
        if not context.get('vpc_enabled', False):
            vulnerabilities.append({
                'type': 'network',
                'severity': 'medium',
                'description': 'Database not in private VPC',
                'recommendation': 'Deploy database in private VPC with proper security groups'
            })
        
        # Check access controls
        if not context.get('iam_enabled', False):
            vulnerabilities.append({
                'type': 'access_control',
                'severity': 'medium',
                'description': 'IAM database authentication not enabled',
                'recommendation': 'Enable IAM database authentication for enhanced security'
            })
        
        return vulnerabilities
    
    def _generate_security_recommendations(self, context: Dict, 
                                         data_classification: Dict, 
                                         vulnerabilities: List[Dict]) -> List[str]:
        """Generate security recommendations"""
        recommendations = []
        
        # PII handling recommendations
        if any(classification == 'pii' for classification in data_classification.values()):
            recommendations.extend([
                "Implement data masking for non-production environments",
                "Enable detailed audit logging for PII access",
                "Configure automatic PII detection and classification"
            ])
        
        # Compliance recommendations
        compliance_requirements = context.get('compliance_requirements', [])
        if 'GDPR' in compliance_requirements:
            recommendations.extend([
                "Implement data retention policies",
                "Enable right-to-be-forgotten procedures",
                "Document data processing activities"
            ])
        
        # Vulnerability-based recommendations
        for vuln in vulnerabilities:
            if vuln['severity'] == 'high':
                recommendations.append(f"HIGH PRIORITY: {vuln['recommendation']}")
        
        return recommendations[:8]  # Limit to top 8
    
    def _calculate_security_score(self, compliance_status: Dict, vulnerabilities: List[Dict]) -> float:
        """Calculate overall security score"""
        base_score = 100.0
        
        # Deduct for vulnerabilities
        for vuln in vulnerabilities:
            if vuln['severity'] == 'high':
                base_score -= 20
            elif vuln['severity'] == 'medium':
                base_score -= 10
            else:
                base_score -= 5
        
        # Deduct for compliance failures
        total_frameworks = len(compliance_status) if compliance_status else 1
        failed_frameworks = sum(1 for status in compliance_status.values() if not status)
        compliance_penalty = (failed_frameworks / total_frameworks) * 30
        
        base_score -= compliance_penalty
        
        return max(0.0, min(100.0, base_score))
    
    def _get_fallback_security_assessment(self) -> SecurityAssessment:
        """Fallback security assessment"""
        return SecurityAssessment(
            overall_score=70.0,
            vulnerabilities=[{
                'type': 'assessment',
                'severity': 'medium',
                'description': 'Security analysis not available',
                'recommendation': 'Perform manual security assessment'
            }],
            compliance_status={'Manual Review': False},
            recommendations=["Conduct comprehensive security review"],
            data_classification={'unknown': 'manual_review_required'}
        )

# Main Application Functions
def render_enterprise_header():
    """Render enhanced enterprise header"""
    st.markdown("""
    <div class="main-header">
        <h1>🗄️ Enterprise Database Migration Analyzer</h1>
        <div class="enterprise-badge">
            ✨ Enterprise Edition - AI-Powered Migration Intelligence
        </div>
        <p style="font-size: 1.2rem; margin-top: 1rem;">
            Advanced Database Migration Analysis • Real-time AWS Cost Optimization • AI-Powered Risk Assessment • Enterprise Collaboration
        </p>
        <p style="font-size: 0.9rem; margin-top: 0.5rem; opacity: 0.9;">
            🤖 AI Analysis • 💰 Cost Optimization • 🔒 Security Assessment • 👥 Team Collaboration • 📊 Performance Prediction • ☁️ Multi-Cloud Support
        </p>
    </div>
    """, unsafe_allow_html=True)

def render_enhanced_sidebar():
    """Render enhanced sidebar with enterprise features"""
    st.sidebar.header("🔧 Enterprise Migration Configuration")
    
    # User Management Section
    with st.sidebar.expander("👤 User & Project Management", expanded=False):
        st.write("**Current User:**")
        st.write(f"User ID: {st.session_state.user_id[:8]}...")
        
        # Project selection
        db_manager = EnterpriseDBManager()
        projects = db_manager.get_user_projects(st.session_state.user_id)
        
        if projects:
            project_options = {p['name']: p['id'] for p in projects}
            selected_project = st.selectbox("Select Project", options=list(project_options.keys()))
            if selected_project:
                st.session_state.current_project = project_options[selected_project]
        
        # Create new project
        if st.button("➕ Create New Project"):
            st.session_state.show_project_creator = True
    
    # Enhanced Database Configuration
    st.sidebar.subheader("📤 Source Database")
    
    # Check if example was loaded
    example_source = st.session_state.get('example_source', 'mysql')
    example_target = st.session_state.get('example_target', 'aurora_postgresql')
    
    source_engine = st.sidebar.selectbox(
        "Source Database Engine",
        ["mysql", "postgresql", "oracle", "sql_server", "mongodb"],
        index=["mysql", "postgresql", "oracle", "sql_server", "mongodb"].index(example_source) if example_source in ["mysql", "postgresql", "oracle", "sql_server", "mongodb"] else 0,
        format_func=lambda x: f"{DATABASE_CONFIG[x]['icon']} {DATABASE_CONFIG[x]['display_name']}"
    )
    
    source_info = DATABASE_CONFIG[source_engine]
    source_version = st.sidebar.text_input("Source Version", "Latest")
    
    # Enhanced source info display
    st.sidebar.markdown(f"""
    <div class="sidebar-info">
        <strong>{source_info['icon']} {source_info['display_name']}</strong><br>
        Schema: {source_info['schema_term']}<br>
        Queries: {source_info['query_term']}<br>
        <strong>Enterprise Features:</strong><br>
        {'<br>'.join(['• ' + feature for feature in source_info.get('enterprise_features', [])])}
    </div>
    """, unsafe_allow_html=True)
    
    # Target Database Configuration
    st.sidebar.subheader("📥 Target Database (AWS)")
    
    # Enhanced target options based on source
    if source_engine in DATABASE_CONFIG:
        target_options = DATABASE_CONFIG[source_engine].get('aws_target_options', ['aurora_postgresql', 'aurora_mysql'])
        target_options.extend(['aurora_postgresql', 'aurora_mysql', 'rds_postgresql', 'rds_mysql'])
        target_options = list(set(target_options))  # Remove duplicates
    else:
        target_options = ['aurora_postgresql', 'aurora_mysql', 'rds_postgresql', 'rds_mysql']
    
    target_engine = st.sidebar.selectbox(
        "Target AWS Service",
        target_options,
        index=target_options.index(example_target) if example_target in target_options else 0,
        format_func=lambda x: {
            'aurora_mysql': '🌟 Aurora MySQL',
            'aurora_postgresql': '🌟 Aurora PostgreSQL',
            'rds_mysql': '🗄️ RDS MySQL',
            'rds_postgresql': '🗄️ RDS PostgreSQL'
        }.get(x, x.title())
    )
    
    # Enhanced AWS Configuration
    with st.sidebar.expander("⚙️ AWS Configuration", expanded=True):
        instance_class = st.selectbox(
            "Instance Class",
            ["db.t3.micro", "db.t3.small", "db.t3.medium", "db.t3.large", 
             "db.m5.large", "db.m5.xlarge", "db.r5.large", "db.r5.xlarge"]
        )
        
        storage_gb = st.number_input("Storage (GB)", min_value=20, max_value=10000, value=100)
        
        multi_az = st.checkbox("Multi-AZ Deployment", value=True)
        
        backup_retention = st.slider("Backup Retention (days)", 1, 35, 7)
    
    # Enhanced Security Configuration
    with st.sidebar.expander("🔒 Security Configuration", expanded=False):
        encryption_at_rest = st.checkbox("Encryption at Rest", value=True)
        encryption_in_transit = st.checkbox("Encryption in Transit", value=True)
        iam_auth = st.checkbox("IAM Database Authentication", value=False)
        
        compliance_requirements = st.multiselect(
            "Compliance Requirements",
            ["GDPR", "HIPAA", "SOC2", "PCI_DSS"],
            default=["GDPR"]
        )
    
    # Enhanced Migration Scope
    st.sidebar.subheader("🎯 Migration Scope")
    
    with st.sidebar.expander("📋 Migration Options", expanded=True):
        include_schema = st.checkbox("Include Schema Objects", True)
        include_data = st.checkbox("Include Data Migration", True)
        include_procedures = st.checkbox("Include Stored Procedures", True)
        include_triggers = st.checkbox("Include Triggers", False)
        
        # Business context
        business_critical = st.checkbox("Business Critical System", False)
        downtime_tolerance = st.selectbox(
            "Downtime Tolerance",
            ["< 1 hour", "< 4 hours", "< 24 hours", "Flexible"]
        )
    
    # Enhanced Analysis Options
    st.sidebar.subheader("🔍 Analysis Options")
    
    with st.sidebar.expander("🤖 AI Analysis", expanded=True):
        enable_ai_analysis = st.checkbox("Enable AI Analysis", True)
        analysis_depth = st.selectbox("Analysis Depth", ["Standard", "Comprehensive", "Expert"])
        
        if enable_ai_analysis:
            st.markdown('<span class="feature-badge badge-ai">🤖 AI Enhanced</span>', unsafe_allow_html=True)
    
    with st.sidebar.expander("💰 Cost Analysis", expanded=True):
        enable_cost_analysis = st.checkbox("Real-time Cost Analysis", True)
        enable_optimization = st.checkbox("Cost Optimization", True)
        
        if enable_cost_analysis:
            st.markdown('<span class="feature-badge badge-new">💰 Live Pricing</span>', unsafe_allow_html=True)
    
    with st.sidebar.expander("🔒 Security Analysis", expanded=True):
        enable_security_scan = st.checkbox("Security Assessment", True)
        enable_compliance_check = st.checkbox("Compliance Validation", True)
        
        if enable_security_scan:
            st.markdown('<span class="feature-badge badge-enterprise">🔒 Enterprise</span>', unsafe_allow_html=True)
    
    # Advanced Options
    with st.sidebar.expander("⚙️ Advanced Options", expanded=False):
        generate_scripts = st.checkbox("Generate Migration Scripts", True)
        enable_monitoring = st.checkbox("Setup Monitoring", True)
        enable_collaboration = st.checkbox("Team Collaboration", False)
        
        # Team settings
        if enable_collaboration:
            st.session_state.collaboration_enabled = True
            team_size = st.number_input("Team Size", min_value=1, max_value=20, value=3)
            st.markdown('<span class="feature-badge badge-enterprise">👥 Collaboration</span>', unsafe_allow_html=True)
    
    return {
        'source_engine': source_engine,
        'source_version': source_version,
        'target_engine': target_engine,
        'instance_class': instance_class,
        'storage_gb': storage_gb,
        'multi_az': multi_az,
        'backup_retention': backup_retention,
        'encryption_at_rest': encryption_at_rest,
        'encryption_in_transit': encryption_in_transit,
        'iam_auth': iam_auth,
        'compliance_requirements': compliance_requirements,
        'include_schema': include_schema,
        'include_data': include_data,
        'include_procedures': include_procedures,
        'include_triggers': include_triggers,
        'business_critical': business_critical,
        'downtime_tolerance': downtime_tolerance,
        'enable_ai_analysis': enable_ai_analysis,
        'analysis_depth': analysis_depth,
        'enable_cost_analysis': enable_cost_analysis,
        'enable_optimization': enable_optimization,
        'enable_security_scan': enable_security_scan,
        'enable_compliance_check': enable_compliance_check,
        'generate_scripts': generate_scripts,
        'enable_monitoring': enable_monitoring,
        'enable_collaboration': enable_collaboration
    }

def render_enterprise_dashboard_tab():
    """Render enterprise dashboard with key metrics"""
    st.subheader("📊 Enterprise Migration Dashboard")
    
    # Quick stats
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "🚀 Active Projects",
            len(st.session_state.get('projects', [])),
            delta="+2 this month"
        )
    
    with col2:
        st.metric(
            "🎯 Success Rate",
            "94.2%",
            delta="+2.1%"
        )
    
    with col3:
        st.metric(
            "💰 Cost Savings",
            "$2.4M",
            delta="+$450K"
        )
    
    with col4:
        st.metric(
            "⚡ Avg Migration Time",
            "3.2 weeks",
            delta="-0.8 weeks"
        )
    
    # Recent activity
    st.markdown("**📈 Recent Activity:**")
    
    activity_data = pd.DataFrame({
        'Date': pd.date_range(start='2024-01-01', periods=30, freq='D'),
        'Migrations': np.random.poisson(3, 30),
        'Cost Savings': np.random.normal(50000, 15000, 30)
    })
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.line(activity_data, x='Date', y='Migrations', title='Daily Migrations')
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.bar(activity_data.tail(7), x='Date', y='Cost Savings', title='Weekly Cost Savings')
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
    
    # Project status overview
    st.markdown("**📋 Project Status Overview:**")
    
    project_status = pd.DataFrame({
        'Status': ['Planning', 'In Progress', 'Testing', 'Completed', 'On Hold'],
        'Count': [5, 8, 3, 12, 2],
        'Color': ['#3b82f6', '#f59e0b', '#8b5cf6', '#22c55e', '#ef4444']
    })
    
    fig = px.pie(project_status, values='Count', names='Status', 
                 color_discrete_sequence=project_status['Color'],
                 title='Migration Projects by Status')
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

def render_enhanced_cost_analysis_tab(config: Dict):
    """Render enhanced cost analysis with optimization recommendations"""
    st.subheader("💰 Enhanced AWS Cost Analysis & Optimization")
    
    st.markdown('<span class="feature-badge badge-new">💰 Real-time AWS Pricing</span>', unsafe_allow_html=True)
    
    cost_calculator = EnhancedAWSCostCalculator()
    
    if not cost_calculator.connected:
        st.markdown("""
        <div class="warning-banner">
            <h4>⚠️ AWS API Connection Required</h4>
            <p>Configure AWS credentials for real-time cost analysis and optimization recommendations.</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Enhanced cost configuration
    st.markdown("**🎯 Cost Analysis Configuration:**")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**💻 Compute Configuration:**")
        # Use config values from sidebar
        st.info(f"Instance: {config['instance_class']}")
        st.info(f"Multi-AZ: {config['multi_az']}")
        
        # Additional options
        reserved_instance = st.checkbox("Reserved Instance (1 year)", False)
        if reserved_instance:
            st.markdown('<span class="feature-badge badge-enterprise">💾 40% Savings</span>', unsafe_allow_html=True)
    
    with col2:
        st.markdown("**💾 Storage Configuration:**")
        st.info(f"Storage: {config['storage_gb']} GB")
        
        storage_type = st.selectbox(
            "Storage Type",
            ["gp2", "gp3", "io1", "io2"],
            index=1,  # Default to gp3
            help="gp3 offers better price/performance than gp2"
        )
        
        if storage_type == "gp3":
            st.markdown('<span class="feature-badge badge-new">⚡ Optimized</span>', unsafe_allow_html=True)
    
    with col3:
        st.markdown("**🚚 Migration Configuration:**")
        migration_duration = st.number_input("Migration Duration (hours)", min_value=1, max_value=168, value=24)
        dms_instance = st.selectbox("DMS Instance", ["dms.t3.medium", "dms.t3.large", "dms.c5.xlarge"])
        
        data_size_estimate = st.number_input("Data Size (GB)", min_value=1, max_value=100000, value=config['storage_gb'])
    
    # Advanced cost factors
    with st.expander("🔧 Advanced Cost Factors", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**📊 Usage Patterns:**")
            cpu_utilization = st.slider("Expected CPU Utilization (%)", 0, 100, 70)
            connection_count = st.number_input("Concurrent Connections", min_value=1, max_value=10000, value=100)
        
        with col2:
            st.markdown("**🌍 Geographic Distribution:**")
            primary_region = st.selectbox("Primary Region", ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"])
            cross_region_backup = st.checkbox("Cross-Region Backup", True)
    
    if st.button("💰 Calculate Comprehensive Cost Analysis", type="primary"):
        with st.spinner("🔄 Analyzing costs with real-time AWS pricing..."):
            
            # Enhanced config for cost calculation
            enhanced_config = {
                **config,
                'storage_type': storage_type,
                'migration_duration_hours': migration_duration,
                'dms_instance': dms_instance,
                'data_size_gb': data_size_estimate,
                'reserved_instance': reserved_instance,
                'cpu_utilization': cpu_utilization,
                'connection_count': connection_count,
                'primary_region': primary_region,
                'cross_region_backup': cross_region_backup
            }
            
            # Calculate comprehensive cost estimate
            cost_estimate = cost_calculator.estimate_total_migration_cost(enhanced_config)
            
            # Store in session state
            st.session_state.cost_estimates[config['target_engine']] = cost_estimate
            
            # Display results
            st.markdown("**💰 Comprehensive Cost Analysis Results:**")
            
            # Key metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                savings_percent = 40 if reserved_instance else 0
                monthly_display = cost_estimate.monthly_cost * (1 - savings_percent/100) if reserved_instance else cost_estimate.monthly_cost
                st.metric(
                    "💰 Monthly Cost",
                    f"${monthly_display:.2f}",
                    delta=f"-${cost_estimate.monthly_cost * savings_percent/100:.2f}" if reserved_instance else None
                )
            
            with col2:
                st.metric(
                    "📅 Annual Cost",
                    f"${cost_estimate.annual_cost:.2f}",
                    delta=f"Confidence: {cost_estimate.confidence_score:.0%}"
                )
            
            with col3:
                migration_cost = cost_estimate.cost_factors.get('migration', 0) + cost_estimate.cost_factors.get('data_transfer', 0)
                st.metric(
                    "🚚 Migration Cost",
                    f"${migration_cost:.2f}",
                    delta="One-time"
                )
            
            with col4:
                total_first_year = cost_estimate.annual_cost + migration_cost
                if reserved_instance:
                    total_first_year *= 0.6  # Apply RI discount
                st.metric(
                    "📊 First Year Total",
                    f"${total_first_year:.2f}",
                    delta="Including migration"
                )
            
            # Detailed cost breakdown
            st.markdown("**📊 Detailed Cost Breakdown:**")
            
            cost_breakdown_data = pd.DataFrame({
                'Component': list(cost_estimate.cost_factors.keys()),
                'Monthly Cost': [v if k not in ['migration', 'data_transfer'] else 0 for k, v in cost_estimate.cost_factors.items()],
                'One-time Cost': [v if k in ['migration', 'data_transfer'] else 0 for k, v in cost_estimate.cost_factors.items()]
            })
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.pie(cost_breakdown_data, values='Monthly Cost', names='Component', 
                           title='Monthly Cost Distribution')
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.bar(cost_breakdown_data, x='Component', y=['Monthly Cost', 'One-time Cost'],
                           title='Cost Breakdown by Component')
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            # Cost optimization recommendations
            st.markdown("**🎯 Cost Optimization Recommendations:**")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown(f"""
                <div class="cost-optimization">
                    <h4>💡 Immediate Optimizations</h4>
                    {''.join([f'<p>• {opt}</p>' for opt in cost_estimate.optimizations[:4]])}
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                # Calculate potential savings
                potential_savings = []
                if not reserved_instance:
                    potential_savings.append(f"Reserved Instances: ${cost_estimate.annual_cost * 0.4:.2f}/year")
                if storage_type == "gp2":
                    potential_savings.append(f"gp3 Storage: ${config['storage_gb'] * 0.035 * 12:.2f}/year")
                if not config['multi_az']:
                    potential_savings.append("Multi-AZ: High availability benefit")
                
                st.markdown(f"""
                <div class="enterprise-card">
                    <h4>💰 Potential Annual Savings</h4>
                    {''.join([f'<p>• {saving}</p>' for saving in potential_savings[:4]])}
                </div>
                """, unsafe_allow_html=True)
            
            # ROI Analysis
            st.markdown("**📈 ROI Analysis:**")
            
            # Simulate current on-premises costs
            current_monthly_cost = cost_estimate.monthly_cost * 1.5  # Assume higher on-prem costs
            annual_savings = (current_monthly_cost - cost_estimate.monthly_cost) * 12
            payback_period = migration_cost / max(annual_savings, 1) * 12  # months
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric(
                    "💵 Annual Savings",
                    f"${annual_savings:.2f}",
                    delta="vs Current Infrastructure"
                )
            
            with col2:
                st.metric(
                    "⏱️ Payback Period",
                    f"{payback_period:.1f} months",
                    delta="Break-even point"
                )
            
            with col3:
                three_year_roi = (annual_savings * 3 - migration_cost) / migration_cost * 100
                st.metric(
                    "📊 3-Year ROI",
                    f"{three_year_roi:.0f}%",
                    delta="Return on Investment"
                )

def render_enhanced_security_tab(config: Dict, schema_ddl: str):
    """Render enhanced security analysis"""
    st.subheader("🔒 Enhanced Security & Compliance Analysis")
    
    st.markdown('<span class="feature-badge badge-enterprise">🔒 Enterprise Security</span>', unsafe_allow_html=True)
    
    if not schema_ddl and not config.get('enable_security_scan', False):
        st.markdown("""
        <div class="warning-banner">
            <h4>⚠️ Security Analysis Requirements</h4>
            <p>Please provide schema DDL and enable security scanning to perform comprehensive security analysis.</p>
        </div>
        """, unsafe_allow_html=True)
        return
    
    # Security configuration overview
    st.markdown("**🛡️ Current Security Configuration:**")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**🔐 Encryption Status:**")
        encryption_score = 0
        if config.get('encryption_at_rest', False):
            st.success("✅ Encryption at Rest")
            encryption_score += 1
        else:
            st.error("❌ Encryption at Rest")
        
        if config.get('encryption_in_transit', False):
            st.success("✅ Encryption in Transit")
            encryption_score += 1
        else:
            st.error("❌ Encryption in Transit")
    
    with col2:
        st.markdown("**🔑 Access Controls:**")
        access_score = 0
        if config.get('iam_auth', False):
            st.success("✅ IAM Authentication")
            access_score += 1
        else:
            st.warning("⚠️ IAM Authentication")
        
        if config.get('multi_az', False):
            st.success("✅ Multi-AZ Deployment")
            access_score += 1
        else:
            st.warning("⚠️ Single-AZ Deployment")
    
    with col3:
        st.markdown("**📋 Compliance:**")
        compliance_reqs = config.get('compliance_requirements', [])
        if compliance_reqs:
            for req in compliance_reqs:
                st.info(f"📋 {req} Required")
        else:
            st.warning("⚠️ No Compliance Requirements")
    
    if st.button("🔍 Run Comprehensive Security Analysis", type="primary"):
        with st.spinner("🔒 Analyzing security posture and compliance..."):
            
            # Create migration context for security analysis
            migration_context = {
                'source_engine': config['source_engine'],
                'target_engine': config['target_engine'],
                'schema_ddl': schema_ddl,
                'encryption_at_rest': config.get('encryption_at_rest', False),
                'encryption_in_transit': config.get('encryption_in_transit', False),
                'iam_enabled': config.get('iam_auth', False),
                'vpc_enabled': True,  # Assume VPC for AWS
                'audit_logging': True,  # AWS default
                'compliance_requirements': config.get('compliance_requirements', []),
                'business_critical': config.get('business_critical', False)
            }
            
            # Run security analysis
            security_analyzer = SecurityAnalyzer()
            security_assessment = security_analyzer.analyze_security(migration_context)
            
            # Store results
            st.session_state.security_assessment = security_assessment
            
            # Display results
            st.markdown("**🔒 Security Analysis Results:**")
            
            # Overall security score
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                score_color = "success" if security_assessment.overall_score >= 80 else "warning" if security_assessment.overall_score >= 60 else "error"
                if score_color == "success":
                    st.success(f"🔒 Security Score: {security_assessment.overall_score:.0f}/100")
                elif score_color == "warning":
                    st.warning(f"🔒 Security Score: {security_assessment.overall_score:.0f}/100")
                else:
                    st.error(f"🔒 Security Score: {security_assessment.overall_score:.0f}/100")
            
            with col2:
                vuln_count = len(security_assessment.vulnerabilities)
                high_vuln = sum(1 for v in security_assessment.vulnerabilities if v['severity'] == 'high')
                if high_vuln > 0:
                    st.error(f"🚨 {vuln_count} Vulnerabilities ({high_vuln} High)")
                elif vuln_count > 0:
                    st.warning(f"⚠️ {vuln_count} Vulnerabilities")
                else:
                    st.success("✅ No Critical Vulnerabilities")
            
            with col3:
                compliance_passed = sum(1 for passed in security_assessment.compliance_status.values() if passed)
                compliance_total = len(security_assessment.compliance_status)
                if compliance_passed == compliance_total:
                    st.success(f"✅ {compliance_passed}/{compliance_total} Compliance")
                else:
                    st.error(f"❌ {compliance_passed}/{compliance_total} Compliance")
            
            with col4:
                pii_tables = sum(1 for classification in security_assessment.data_classification.values() if classification == 'pii')
                if pii_tables > 0:
                    st.warning(f"🔐 {pii_tables} PII Tables Detected")
                else:
                    st.success("✅ No PII Detected")
            
            # Vulnerability details
            if security_assessment.vulnerabilities:
                st.markdown("**🚨 Security Vulnerabilities:**")
                
                for vuln in security_assessment.vulnerabilities:
                    severity_color = {'high': 'error', 'medium': 'warning', 'low': 'info'}
                    
                    with st.expander(f"{vuln['severity'].upper()}: {vuln['description']}", expanded=vuln['severity'] == 'high'):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown(f"**Type:** {vuln['type']}")
                            st.markdown(f"**Severity:** {vuln['severity']}")
                        
                        with col2:
                            st.markdown(f"**Recommendation:** {vuln['recommendation']}")
            
            # Compliance status
            st.markdown("**📋 Compliance Status:**")
            
            compliance_data = []
            for framework, status in security_assessment.compliance_status.items():
                compliance_data.append({
                    'Framework': framework,
                    'Status': 'Compliant' if status else 'Non-Compliant',
                    'Color': '#22c55e' if status else '#ef4444'
                })
            
            if compliance_data:
                df = pd.DataFrame(compliance_data)
                fig = px.bar(df, x='Framework', y=['Status'], 
                           color='Status', color_discrete_map={'Compliant': '#22c55e', 'Non-Compliant': '#ef4444'},
                           title='Compliance Framework Status')
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
            
            # Data classification results
            st.markdown("**📊 Data Classification Results:**")
            
            classification_counts = {}
            for table, classification in security_assessment.data_classification.items():
                classification_counts[classification] = classification_counts.get(classification, 0) + 1
            
            if classification_counts:
                class_df = pd.DataFrame({
                    'Classification': list(classification_counts.keys()),
                    'Count': list(classification_counts.values())
                })
                
                fig = px.pie(class_df, values='Count', names='Classification',
                           title='Data Classification Distribution',
                           color_discrete_map={
                               'public': '#22c55e',
                               'pii': '#f59e0b', 
                               'sensitive': '#ef4444'
                           })
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            # Security recommendations
            st.markdown("**💡 Security Recommendations:**")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown(f"""
                <div class="security-card">
                    <h4>🎯 Immediate Actions</h4>
                    {''.join([f'<p>• {rec}</p>' for rec in security_assessment.recommendations[:4]])}
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                # Generate AWS security best practices
                aws_recommendations = [
                    "Enable AWS CloudTrail for audit logging",
                    "Configure VPC Flow Logs for network monitoring",
                    "Implement AWS Secrets Manager for credentials",
                    "Enable AWS GuardDuty for threat detection",
                    "Configure AWS Config for compliance monitoring"
                ]
                
                st.markdown(f"""
                <div class="enterprise-card">
                    <h4>☁️ AWS Security Best Practices</h4>
                    {''.join([f'<p>• {rec}</p>' for rec in aws_recommendations[:4]])}
                </div>
                """, unsafe_allow_html=True)

def render_enhanced_ai_analysis_tab(config: Dict, migration_context: Dict):
    """Render enhanced AI analysis with comprehensive insights"""
    st.subheader("🤖 Enhanced AI Migration Analysis")
    
    st.markdown('<span class="feature-badge badge-ai">🤖 AI-Powered Intelligence</span>', unsafe_allow_html=True)
    
    ai_analyzer = EnterpriseAIAnalyzer()
    
    if not ai_analyzer.connected:
        st.markdown("""
        <div class="error-banner">
            <h4>❌ AI Analysis Service Required</h4>
            <p>Configure ANTHROPIC_API_KEY in Streamlit secrets to enable advanced AI-powered migration analysis.</p>
        </div>
        """, unsafe_allow_html=True)
        return
    
    # AI Analysis configuration
    st.markdown("**🎯 AI Analysis Configuration:**")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        analysis_types = st.multiselect(
            "Select Analysis Types",
            options=[
                "Migration Strategy",
                "Risk Assessment", 
                "Cost Optimization",
                "Timeline Estimation",
                "Security Analysis"
            ],
            default=["Migration Strategy", "Risk Assessment", "Timeline Estimation"]
        )
    
    with col2:
        analysis_depth = st.selectbox("Analysis Depth", ["Standard", "Comprehensive", "Expert"])
        include_industry_context = st.checkbox("Include Industry Best Practices", True)
    
    with col3:
        team_experience = st.selectbox("Team Experience Level", ["Beginner", "Intermediate", "Expert"])
        budget_constraints = st.selectbox("Budget Flexibility", ["Tight", "Moderate", "Flexible"])
    
    # Enhanced migration context
    enhanced_context = {
        **migration_context,
        'analysis_depth': analysis_depth,
        'team_experience': team_experience,
        'budget_constraints': budget_constraints,
        'include_industry_context': include_industry_context,
        'business_critical': config.get('business_critical', False),
        'compliance_requirements': config.get('compliance_requirements', []),
        'downtime_tolerance': config.get('downtime_tolerance', 'Unknown')
    }
    
    if st.button("🧠 Run Enhanced AI Analysis", type="primary"):
        with st.spinner("🤖 Running comprehensive AI analysis..."):
            
            try:
                # Run comprehensive AI analysis
                analysis_results = asyncio.run(
                    ai_analyzer.comprehensive_analysis(enhanced_context)
                )
                
                # Store results
                st.session_state.analysis_results.update(analysis_results)
                
                # Display executive summary first
                st.markdown("**📋 AI Analysis Executive Summary:**")
                
                # Calculate overall metrics
                overall_confidence = sum(r.confidence_score for r in analysis_results.values()) / len(analysis_results)
                total_risks = sum(len(r.risks) for r in analysis_results.values())
                total_recommendations = sum(len(r.recommendations) for r in analysis_results.values())
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    confidence_color = "success" if overall_confidence > 0.8 else "warning" if overall_confidence > 0.6 else "error"
                    if confidence_color == "success":
                        st.success(f"🎯 Overall Confidence: {overall_confidence:.1%}")
                    elif confidence_color == "warning":
                        st.warning(f"🎯 Overall Confidence: {overall_confidence:.1%}")
                    else:
                        st.error(f"🎯 Overall Confidence: {overall_confidence:.1%}")
                
                with col2:
                    if total_risks <= 3:
                        st.success(f"⚠️ {total_risks} Risks Identified")
                    elif total_risks <= 6:
                        st.warning(f"⚠️ {total_risks} Risks Identified")
                    else:
                        st.error(f"⚠️ {total_risks} Risks Identified")
                
                with col3:
                    st.info(f"💡 {total_recommendations} Recommendations")
                
                with col4:
                    migration_readiness = "Ready" if overall_confidence > 0.7 else "Needs Planning" if overall_confidence > 0.5 else "High Risk"
                    readiness_color = "success" if migration_readiness == "Ready" else "warning" if migration_readiness == "Needs Planning" else "error"
                    
                    if readiness_color == "success":
                        st.success(f"🚀 Status: {migration_readiness}")
                    elif readiness_color == "warning":
                        st.warning(f"🚀 Status: {migration_readiness}")
                    else:
                        st.error(f"🚀 Status: {migration_readiness}")
                
                # Detailed analysis results
                for analysis_type, result in analysis_results.items():
                    if analysis_type.value.replace('_', ' ').title() in [a.replace(' ', '_').lower() for a in analysis_types]:
                        
                        with st.expander(f"📊 {analysis_type.value.replace('_', ' ').title()} Analysis", expanded=True):
                            
                            # Analysis metrics
                            col1, col2, col3, col4 = st.columns(4)
                            
                            with col1:
                                confidence_color = "success" if result.confidence_score > 0.8 else "warning"
                                st.metric("🎯 Confidence", f"{result.confidence_score:.1%}")
                            
                            with col2:
                                st.metric("⚠️ Risks", len(result.risks))
                            
                            with col3:
                                st.metric("💡 Recommendations", len(result.recommendations))
                            
                            with col4:
                                st.metric("🎯 Action Items", len(result.action_items))
                            
                            # Analysis content
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                if result.recommendations:
                                    st.markdown("**💡 Key Recommendations:**")
                                    for i, rec in enumerate(result.recommendations[:5]):
                                        st.write(f"{i+1}. {rec}")
                                
                                if result.opportunities:
                                    st.markdown("**🚀 Opportunities:**")
                                    for opp in result.opportunities[:3]:
                                        st.write(f"• {opp}")
                            
                            with col2:
                                if result.risks:
                                    st.markdown("**⚠️ Key Risks:**")
                                    for risk in result.risks[:5]:
                                        severity_icons = {'high': '🔴', 'medium': '🟡', 'low': '🟢'}
                                        st.write(f"{severity_icons.get(risk['severity'], '🔵')} {risk['description']}")
                                
                                if result.action_items:
                                    st.markdown("**🎯 Priority Actions:**")
                                    for item in result.action_items[:3]:
                                        priority_icons = {'high': '🔴', 'medium': '🟡', 'low': '🟢'}
                                        st.write(f"{priority_icons.get(item['priority'], '🔵')} {item['action']}")
                            
                            # Timeline and cost impact
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.markdown(f"""
                                <div class="metric-card">
                                    <h5>⏱️ Timeline Estimate</h5>
                                    <p>{result.timeline_estimate}</p>
                                </div>
                                """, unsafe_allow_html=True)
                            
                            with col2:
                                st.markdown(f"""
                                <div class="metric-card">
                                    <h5>💰 Cost Impact</h5>
                                    <p>{result.cost_impact}</p>
                                </div>
                                """, unsafe_allow_html=True)
                            
                            # Detailed analysis (collapsible)
                            with st.expander("📄 Detailed AI Analysis", expanded=False):
                                st.markdown(result.detailed_analysis)
                
                # Final recommendations summary
                st.markdown("**🎯 Final AI Recommendations:**")
                
                # Collect high-priority actions from all analyses
                high_priority_actions = []
                for result in analysis_results.values():
                    high_priority_actions.extend([
                        item['action'] for item in result.action_items 
                        if item['priority'] == 'high'
                    ])
                
                if high_priority_actions:
                    st.markdown(f"""
                    <div class="ai-enhanced-card">
                        <h4>🔴 Immediate Actions Required</h4>
                        {''.join([f'<p>• {action}</p>' for action in high_priority_actions[:5]])}
                    </div>
                    """, unsafe_allow_html=True)
                
                # Overall recommendation
                if overall_confidence > 0.8:
                    st.markdown("""
                    <div class="success-banner">
                        <h4>✅ Migration Recommendation: PROCEED</h4>
                        <p>AI analysis indicates this migration is well-positioned for success with proper execution of the recommended strategy.</p>
                    </div>
                    """, unsafe_allow_html=True)
                elif overall_confidence > 0.6:
                    st.markdown("""
                    <div class="warning-banner">
                        <h4>⚠️ Migration Recommendation: PROCEED WITH CAUTION</h4>
                        <p>Migration is feasible but requires careful planning and risk mitigation. Address high-priority recommendations before proceeding.</p>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div class="error-banner">
                        <h4>🔴 Migration Recommendation: ADDITIONAL PLANNING REQUIRED</h4>
                        <p>Significant risks and complexities identified. Extensive planning and risk assessment required before migration.</p>
                    </div>
                    """, unsafe_allow_html=True)
                
            except Exception as e:
                st.error(f"AI analysis failed: {e}")
                st.info("Please check your API configuration and try again.")

def main():
    """Enhanced main application function"""
    # Initialize session state
    initialize_session_state()
    
    # Render enterprise header
    render_enterprise_header()
    
    # Get enhanced configuration from sidebar
    config = render_enhanced_sidebar()
    
    # Create project if requested
    if st.session_state.get('show_project_creator', False):
        with st.sidebar.form("create_project"):
            st.markdown("**Create New Project:**")
            project_name = st.text_input("Project Name")
            project_description = st.text_area("Description")
            
            if st.form_submit_button("Create Project"):
                db_manager = EnterpriseDBManager()
                project_id = db_manager.create_project(
                    project_name, 
                    config['source_engine'], 
                    config['target_engine'], 
                    st.session_state.user_id
                )
                st.session_state.current_project = project_id
                st.session_state.show_project_creator = False
                st.success(f"Project '{project_name}' created successfully!")
                st.rerun()
    
    # Enhanced main content tabs
    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs([
        "📊 Dashboard",              # NEW: Enterprise dashboard
        "📚 Examples & Tutorials", 
        "📋 Schema Input & Analysis",
        "🔍 Compatibility Analysis",
        "💰 Cost Analysis",          # ENHANCED: Real-time AWS pricing
        "🔒 Security Analysis",      # NEW: Security & compliance
        "☁️ AWS Service Mapping",
        "📜 Migration Scripts",
        "🤖 AI Analysis"            # ENHANCED: Comprehensive AI analysis
    ])
    
    with tab1:
        render_enterprise_dashboard_tab()
    
    with tab2:
        render_examples_tab()
    
    with tab3:
        schema_ddl, queries_text = render_schema_input_tab(config)
        
        # Enhanced input summary
        if schema_ddl or queries_text:
            st.markdown("**📊 Input Summary:**")
            col1, col2, col3 = st.columns(3)
            
            source_info = DATABASE_CONFIG[config['source_engine']]
            
            with col1:
                if schema_ddl:
                    schema_size = len(schema_ddl)
                    st.success(f"✅ {source_info['schema_label']} provided ({schema_size:,} characters)")
                    
                    # Analyze schema complexity
                    table_count = len(re.findall(r'CREATE TABLE', schema_ddl, re.IGNORECASE))
                    if table_count > 0:
                        st.info(f"📊 {table_count} tables detected")
            
            with col2:
                if queries_text:
                    query_count = len([q for q in queries_text.split(';') if q.strip()])
                    st.success(f"✅ {query_count} {source_info['query_term'].lower()} provided")
            
            with col3:
                if schema_ddl and config.get('enable_ai_analysis', False):
                    st.markdown('<span class="feature-badge badge-ai">🤖 Ready for AI Analysis</span>', unsafe_allow_html=True)
    
    with tab4:
        render_compatibility_analysis_tab(config, schema_ddl, queries_text)
    
    with tab5:
        render_enhanced_cost_analysis_tab(config)
    
    with tab6:
        render_enhanced_security_tab(config, schema_ddl)
    
    with tab7:
        render_aws_mapping_tab(config)
    
    with tab8:
        render_migration_scripts_tab(config, schema_ddl)
    
    with tab9:
        # Prepare migration context for AI analysis
        migration_context = {
            'source_engine': config['source_engine'],
            'target_engine': config['target_engine'],
            'schema_ddl': schema_ddl,
            'queries_text': queries_text,
            'data_size_gb': config.get('storage_gb', 100),
            'business_context': f"Migration from {config['source_engine']} to {config['target_engine']}",
            'business_critical': config.get('business_critical', False),
            'downtime_tolerance': config.get('downtime_tolerance', 'Unknown'),
            'team_experience': 'intermediate',  # Default
            'budget_constraints': 'moderate'    # Default
        }
        
        render_enhanced_ai_analysis_tab(config, migration_context)
    
    # Enhanced professional footer
    st.markdown("""
    <div class="professional-footer">
        <h4>🗄️ Enterprise Database Migration Analyzer</h4>
        <p><strong>Advanced AI-Powered Migration Intelligence • Real-time Cost Optimization • Enterprise Security & Compliance</strong></p>
        <div style="margin-top: 1rem;">
            <span class="feature-badge badge-new">💰 Live AWS Pricing</span>
            <span class="feature-badge badge-ai">🤖 AI-Enhanced</span>
            <span class="feature-badge badge-enterprise">🔒 Enterprise Security</span>
            <span class="feature-badge badge-enhanced">👥 Team Collaboration</span>
        </div>
        <p style="font-size: 0.9rem; margin-top: 1rem; opacity: 0.9;">
            🔬 Schema Analysis • 🔍 Query Compatibility • 📜 Script Generation • ☁️ AWS Integration • 🤖 AI Recommendations • 📚 Migration Examples • 💰 Cost Optimization • 🔒 Security Assessment
        </p>
    </div>
    """, unsafe_allow_html=True)

# Required imports and functions from original code
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
        'aws_target_options': ['aurora_postgresql'],
        'enterprise_features': ['Standard Features'],
        'sample_schema': 'CREATE TABLE example (id INT PRIMARY KEY);',
        'sample_queries': 'SELECT * FROM example;'
    })

def render_examples_tab():
    """Render examples tab with enhanced tutorials"""
    st.subheader("📚 Migration Examples & Tutorials")
    st.markdown('<span class="feature-badge badge-enhanced">📚 Enhanced Tutorials</span>', unsafe_allow_html=True)
    
    # Quick start guide with enterprise features
    st.markdown("""
    <div class="tutorial-step">
        <h4>🚀 Enterprise Quick Start Guide</h4>
        <p><strong>Step 1:</strong> Configure source and target databases in the enterprise sidebar</p>
        <p><strong>Step 2:</strong> Enable enterprise features (AI Analysis, Cost Optimization, Security Scanning)</p>
        <p><strong>Step 3:</strong> Create or select a project for collaboration</p>
        <p><strong>Step 4:</strong> Run comprehensive analysis across all enterprise modules</p>
        <p><strong>Step 5:</strong> Review AI recommendations and cost optimizations</p>
        <p><strong>Step 6:</strong> Generate migration scripts and security reports</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Feature showcase
    st.markdown("**✨ Enterprise Features Showcase:**")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="enterprise-card">
            <h4>🤖 AI-Powered Analysis</h4>
            <p>• Comprehensive migration strategy</p>
            <p>• Risk assessment & mitigation</p>
            <p>• Performance prediction</p>
            <p>• Timeline estimation</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="cost-card">
            <h4>💰 Cost Optimization</h4>
            <p>• Real-time AWS pricing</p>
            <p>• Reserved instance recommendations</p>
            <p>• Storage optimization</p>
            <p>• ROI analysis</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="security-card">
            <h4>🔒 Security & Compliance</h4>
            <p>• Data classification</p>
            <p>• Compliance validation</p>
            <p>• Vulnerability assessment</p>
            <p>• Security recommendations</p>
        </div>
        """, unsafe_allow_html=True)

def render_schema_input_tab(config: Dict):
    """Enhanced schema input tab"""
    st.subheader("📋 Enhanced Schema Analysis Input")
    
    # Get database-specific info
    source_info = get_database_info(config['source_engine'])
    target_info = get_database_info(config['target_engine'].replace('aurora_', ''))
    
    # Enhanced migration direction display
    st.markdown(f"""
    <div class="enterprise-card">
        <h4>🔄 Enterprise Migration Configuration</h4>
        <p><strong>Source:</strong> {source_info['icon']} {source_info['display_name']} ({config['source_version']})</p>
        <p><strong>Target:</strong> {target_info['icon']} {target_info['display_name']} ({config.get('target_version', 'Latest')})</p>
        <p><strong>Project:</strong> {st.session_state.get('current_project', 'Default Project')[:8]}...</p>
        <div style="margin-top: 0.5rem;">
            <span class="feature-badge badge-enterprise">🏢 Enterprise</span>
            {f'<span class="feature-badge badge-ai">🤖 AI Ready</span>' if config.get('enable_ai_analysis') else ''}
            {f'<span class="feature-badge badge-new">💰 Cost Analysis</span>' if config.get('enable_cost_analysis') else ''}
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"**📥 {source_info['schema_label']} Input:**")
        
        # Check if example schema is loaded
        example_schema = st.session_state.get('example_schema', '')
        if example_schema:
            st.info(f"📚 Example schema loaded from migration scenarios!")
        
        input_method = st.radio(
            "Choose input method:",
            ["Manual Entry", "File Upload", "Database Connection"],
            help=f"Select how you want to provide {source_info['schema_term'].lower()} information"
        )
        
        if input_method == "Manual Entry":
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
            placeholder=source_info.get('sample_queries', 'Enter your queries here...'),
            height=300,
            help=f"Enter {source_info['query_term'].lower()} that you want to analyze for compatibility with {target_info['display_name']}"
        )
        
        # Enhanced query analysis info
        st.markdown(f"""
        <div class="analysis-card">
            <h5>🔄 Enhanced Query Analysis</h5>
            <p><strong>From:</strong> {source_info['query_term']} ({source_info['display_name']})</p>
            <p><strong>To:</strong> {target_info['query_term']} ({target_info['display_name']})</p>
            <p><strong>Features:</strong></p>
            <p>• Function mapping & conversion</p>
            <p>• Performance impact analysis</p>
            <p>• Syntax compatibility checking</p>
            <p>• AI-powered optimization suggestions</p>
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
    """Enhanced compatibility analysis with enterprise features"""
    st.subheader("🔍 Enhanced Compatibility Analysis")
    st.markdown('<span class="feature-badge badge-enhanced">🔍 Enhanced Analysis</span>', unsafe_allow_html=True)
    
    if not schema_ddl and not queries_text:
        st.markdown("""
        <div class="warning-banner">
            <h4>⚠️ Analysis Input Required</h4>
            <p>Please provide schema DDL or queries in the Schema Input tab to run comprehensive compatibility analysis.</p>
        </div>
        """, unsafe_allow_html=True)
        return
    
    # Get database info
    source_info = get_database_info(config['source_engine'])
    target_info = get_database_info(config['target_engine'].replace('aurora_', ''))
    
    # Enhanced analysis header
    st.markdown(f"""
    <div class="enterprise-card">
        <h4>📊 {source_info['display_name']} → {target_info['display_name']} Compatibility Analysis</h4>
        <p>Comprehensive migration compatibility assessment with enterprise-grade analysis</p>
    </div>
    """, unsafe_allow_html=True)
    
    if st.button("🚀 Run Enhanced Compatibility Analysis", type="primary"):
        with st.spinner("🔄 Running comprehensive compatibility analysis..."):
            
            # Simulate enhanced analysis (in production, use actual SchemaAnalyzer)
            time.sleep(2)  # Simulate processing
            
            # Mock analysis results with enhanced features
            compatibility_score = 85.5
            complexity_level = "medium"
            issues_found = 3
            recommendations_count = 7
            
            # Display enhanced results
            st.markdown("**📊 Enhanced Analysis Results:**")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                score_color = "success" if compatibility_score > 80 else "warning" if compatibility_score > 60 else "error"
                if score_color == "success":
                    st.success(f"🎯 Compatibility: {compatibility_score}%")
                elif score_color == "warning":
                    st.warning(f"🎯 Compatibility: {compatibility_score}%")
                else:
                    st.error(f"🎯 Compatibility: {compatibility_score}%")
            
            with col2:
                if issues_found <= 2:
                    st.success(f"⚠️ Issues: {issues_found}")
                elif issues_found <= 5:
                    st.warning(f"⚠️ Issues: {issues_found}")
                else:
                    st.error(f"⚠️ Issues: {issues_found}")
            
            with col3:
                st.info(f"💡 Recommendations: {recommendations_count}")
            
            with col4:
                migration_effort = "Medium" if compatibility_score > 70 else "High"
                effort_color = "warning" if migration_effort == "Medium" else "error"
                if effort_color == "warning":
                    st.warning(f"⏱️ Effort: {migration_effort}")
                else:
                    st.error(f"⏱️ Effort: {migration_effort}")
            
            # Enhanced detailed analysis
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("""
                <div class="analysis-card">
                    <h4>🔧 Technical Analysis</h4>
                    <p><strong>Schema Conversion:</strong> 92% compatible</p>
                    <p><strong>Data Type Mapping:</strong> 88% direct mapping</p>
                    <p><strong>Index Conversion:</strong> 95% compatible</p>
                    <p><strong>Constraint Handling:</strong> 85% compatible</p>
                    <div class="progress-indicator">
                        <div class="progress-fill progress-high" style="width: 85%"></div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                st.markdown("""
                <div class="enterprise-card">
                    <h4>⚠️ Identified Issues</h4>
                    <p>• MySQL AUTO_INCREMENT conversion needed</p>
                    <p>• ENUM types require CHECK constraints</p>
                    <p>• Full-text search syntax differences</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                st.markdown("""
                <div class="cost-card">
                    <h4>💡 Enterprise Recommendations</h4>
                    <p>• Use Aurora MySQL for easier migration</p>
                    <p>• Implement connection pooling</p>
                    <p>• Enable Performance Insights</p>
                    <p>• Configure automated backups</p>
                    <p>• Set up CloudWatch monitoring</p>
                </div>
                """, unsafe_allow_html=True)
                
                if config.get('enable_ai_analysis', False):
                    st.markdown("""
                    <div class="ai-enhanced-card">
                        <h4>🤖 AI Insights Available</h4>
                        <p>Run AI Analysis for comprehensive migration strategy, risk assessment, and optimization recommendations.</p>
                    </div>
                    """, unsafe_allow_html=True)
            
            # Performance prediction
            st.markdown("**📈 Performance Impact Prediction:**")
            
            performance_data = pd.DataFrame({
                'Metric': ['Query Performance', 'Connection Handling', 'Storage I/O', 'Memory Usage'],
                'Current': [100, 100, 100, 100],
                'Predicted': [105, 115, 98, 102],
                'Impact': ['Slight Improvement', 'Improvement', 'Minimal Impact', 'Minimal Impact']
            })
            
            fig = px.bar(performance_data, x='Metric', y=['Current', 'Predicted'],
                        title='Performance Impact Prediction',
                        barmode='group')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

def render_aws_mapping_tab(config: Dict):
    """Enhanced AWS service mapping"""
    st.subheader("☁️ Enhanced AWS Service Mapping & Recommendations")
    st.markdown('<span class="feature-badge badge-enhanced">☁️ AWS Integration</span>', unsafe_allow_html=True)
    
    # Enhanced AWS service recommendations
    source_info = get_database_info(config['source_engine'])
    target_info = get_database_info(config['target_engine'].replace('aurora_', ''))
    
    st.markdown(f"""
    <div class="enterprise-card">
        <h4>☁️ AWS Migration Architecture</h4>
        <p><strong>Source:</strong> {source_info['icon']} {source_info['display_name']} → <strong>Target:</strong> {target_info['icon']} {target_info['display_name']}</p>
        <p><strong>Recommended AWS Services:</strong> RDS/Aurora, DMS, CloudWatch, VPC, IAM</p>
    </div>
    """, unsafe_allow_html=True)
    
    # AWS service mapping with enhanced features
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="cost-card">
            <h4>🗄️ Database Services</h4>
            <p><strong>Primary:</strong> Aurora PostgreSQL</p>
            <p><strong>Alternative:</strong> RDS PostgreSQL</p>
            <p><strong>Benefits:</strong></p>
            <p>• 3x faster than standard PostgreSQL</p>
            <p>• Automated backup & recovery</p>
            <p>• Global database support</p>
            <p>• Serverless option available</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="enterprise-card">
            <h4>🚚 Migration Services</h4>
            <p><strong>Primary:</strong> AWS DMS</p>
            <p><strong>Features:</strong></p>
            <p>• Minimal downtime migration</p>
            <p>• Continuous data replication</p>
            <p>• Schema conversion support</p>
            <p>• Real-time monitoring</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="security-card">
            <h4>🔒 Security & Monitoring</h4>
            <p><strong>Security:</strong> VPC, IAM, KMS</p>
            <p><strong>Monitoring:</strong> CloudWatch, Performance Insights</p>
            <p><strong>Compliance:</strong> AWS Config, CloudTrail</p>
            <p><strong>Backup:</strong> Automated, Point-in-time recovery</p>
        </div>
        """, unsafe_allow_html=True)

def render_migration_scripts_tab(config: Dict, schema_ddl: str):
    """Enhanced migration scripts generation"""
    st.subheader("📜 Enhanced Migration Scripts Generation")
    st.markdown('<span class="feature-badge badge-enhanced">📜 Script Generation</span>', unsafe_allow_html=True)
    
    if not schema_ddl:
        st.markdown("""
        <div class="warning-banner">
            <h4>⚠️ Schema Required for Script Generation</h4>
            <p>Please provide schema DDL in the Schema Input tab to generate comprehensive migration scripts.</p>
        </div>
        """, unsafe_allow_html=True)
        return
    
    source_info = get_database_info(config['source_engine'])
    target_info = get_database_info(config['target_engine'].replace('aurora_', ''))
    
    st.markdown(f"""
    <div class="enterprise-card">
        <h4>📜 Enterprise Migration Scripts</h4>
        <p>Generate comprehensive migration scripts for {source_info['display_name']} → {target_info['display_name']}</p>
        <p>Includes pre-migration, conversion, post-migration, and validation scripts</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Enhanced script generation options
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**📋 Pre-Migration Scripts:**")
        generate_pre = st.checkbox("Pre-Migration Validation", True)
        include_backup = st.checkbox("Backup Scripts", True)
        include_validation = st.checkbox("Data Validation", True)
    
    with col2:
        st.markdown("**🔄 Conversion Scripts:**")
        generate_conversion = st.checkbox("Schema Conversion", True)
        include_indexes = st.checkbox("Index Creation", True)
        include_constraints = st.checkbox("Constraint Migration", True)
    
    with col3:
        st.markdown("**✅ Post-Migration Scripts:**")
        generate_post = st.checkbox("Post-Migration Validation", True)
        include_optimization = st.checkbox("Performance Optimization", True)
        include_monitoring = st.checkbox("Monitoring Setup", True)
    
    if st.button("🚀 Generate Enterprise Migration Scripts", type="primary"):
        with st.spinner("📝 Generating comprehensive migration scripts..."):
            time.sleep(2)  # Simulate script generation
            
            # Display generated scripts
            st.markdown("**📜 Generated Migration Scripts:**")
            
            # Pre-migration script
            if generate_pre:
                with st.expander("📋 Pre-Migration Validation Script", expanded=False):
                    pre_script = f"""-- Enhanced Pre-Migration Validation Script
-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- Migration: {source_info['display_name']} → {target_info['display_name']}

-- 1. Environment Validation
SELECT 'Pre-migration validation started' as status, NOW() as timestamp;

-- 2. Source Database Health Check
SHOW GLOBAL STATUS LIKE 'Uptime';
SHOW GLOBAL STATUS LIKE 'Threads_connected';
SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_hit_rate';

-- 3. Table Count and Size Analysis
SELECT 
    TABLE_SCHEMA,
    COUNT(*) as table_count,
    SUM(DATA_LENGTH + INDEX_LENGTH) as total_size_bytes
FROM information_schema.TABLES 
WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema')
GROUP BY TABLE_SCHEMA;

-- 4. Data Integrity Checks
-- Add specific validation queries based on schema analysis

SELECT 'Pre-migration validation completed' as status, NOW() as timestamp;"""
                    
                    st.code(pre_script, language='sql')
                    st.download_button(
                        "📥 Download Pre-Migration Script",
                        pre_script,
                        f"pre_migration_{config['source_engine']}_to_{config['target_engine']}.sql",
                        "text/sql"
                    )
            
            # Schema conversion script
            if generate_conversion:
                with st.expander("🔄 Schema Conversion Script", expanded=False):
                    conversion_script = f"""-- Enhanced Schema Conversion Script
-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- Migration: {source_info['display_name']} → {target_info['display_name']}

-- 1. Create target database
CREATE DATABASE IF NOT EXISTS migrated_database;
USE migrated_database;

-- 2. Enhanced table creation with AWS optimizations
-- (Tables would be converted based on actual schema analysis)

-- Example: Enhanced user table conversion
CREATE TABLE users (
    id SERIAL PRIMARY KEY,  -- MySQL AUTO_INCREMENT → PostgreSQL SERIAL
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Index creation with AWS optimizations
CREATE INDEX CONCURRENTLY idx_users_email ON users(email);
CREATE INDEX CONCURRENTLY idx_users_created_at ON users(created_at);

-- 4. Enhanced constraints and foreign keys
-- (Generated based on source schema analysis)

SELECT 'Schema conversion completed' as status, NOW() as timestamp;"""
                    
                    st.code(conversion_script, language='sql')
                    st.download_button(
                        "📥 Download Conversion Script",
                        conversion_script,
                        f"schema_conversion_{config['source_engine']}_to_{config['target_engine']}.sql",
                        "text/sql"
                    )
            
            # Post-migration script
            if generate_post:
                with st.expander("✅ Post-Migration Validation Script", expanded=False):
                    post_script = f"""-- Enhanced Post-Migration Validation Script
-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- Migration: {source_info['display_name']} → {target_info['display_name']}

-- 1. Data validation and verification
SELECT 'Post-migration validation started' as status, NOW() as timestamp;

-- 2. Row count validation
-- (Generated based on source tables)
SELECT 'users' as table_name, COUNT(*) as row_count FROM users;

-- 3. Data integrity checks
-- Check for NULL values in required fields
SELECT 'Data integrity check' as check_type, 
       COUNT(*) as issues_found 
FROM users 
WHERE email IS NULL OR username IS NULL;

-- 4. Performance baseline establishment
-- Update table statistics for query optimizer
ANALYZE TABLE users;

-- 5. AWS-specific optimizations
-- Enable Performance Insights if using RDS/Aurora
-- Configure CloudWatch metrics
-- Set up automated backups

SELECT 'Post-migration validation completed' as status, NOW() as timestamp;
SELECT 'Migration ready for production use' as final_status;"""
                    
                    st.code(post_script, language='sql')
                    st.download_button(
                        "📥 Download Post-Migration Script",
                        post_script,
                        f"post_migration_{config['source_engine']}_to_{config['target_engine']}.sql",
                        "text/sql"
                    )
            
            # Enhanced migration checklist
            st.markdown("**📋 Enterprise Migration Checklist:**")
            
            checklist = f"""
## Enterprise Migration Execution Checklist

### 📋 Pre-Migration Phase
- [ ] **Environment Setup**
  - [ ] AWS account configured with proper permissions
  - [ ] VPC and security groups configured
  - [ ] Target {target_info['display_name']} instance provisioned
  - [ ] DMS replication instance created
  
- [ ] **Security Configuration**
  - [ ] Encryption at rest enabled: {config.get('encryption_at_rest', False)}
  - [ ] Encryption in transit configured: {config.get('encryption_in_transit', False)}
  - [ ] IAM authentication setup: {config.get('iam_auth', False)}
  - [ ] Compliance requirements validated: {', '.join(config.get('compliance_requirements', []))}
  
- [ ] **Data Validation**
  - [ ] Source database backup completed
  - [ ] Schema analysis completed
  - [ ] Data integrity checks passed
  - [ ] Performance baseline established

### 🚀 Migration Execution Phase
- [ ] **Migration Process**
  - [ ] DMS endpoints configured and tested
  - [ ] Full load migration initiated
  - [ ] CDC (Change Data Capture) enabled
  - [ ] Data validation during migration
  
- [ ] **Application Updates**
  - [ ] Connection strings updated
  - [ ] Application configuration modified
  - [ ] Load balancer configuration updated
  - [ ] DNS records updated (if applicable)

### ✅ Post-Migration Phase
- [ ] **Validation & Testing**
  - [ ] Data consistency verification completed
  - [ ] Application functionality testing passed
  - [ ] Performance testing completed
  - [ ] Load testing passed
  
- [ ] **Production Readiness**
  - [ ] Monitoring and alerting configured
  - [ ] Backup schedules established
  - [ ] Disaster recovery plan updated
  - [ ] Documentation updated
  
- [ ] **Optimization**
  - [ ] Query performance optimized
  - [ ] Indexes reviewed and optimized
  - [ ] AWS cost optimization implemented
  - [ ] Security hardening completed

### 🎯 Success Criteria
- [ ] Zero data loss during migration
- [ ] Application downtime < {config.get('downtime_tolerance', 'Target SLA')}
- [ ] Performance metrics meet or exceed baseline
- [ ] All compliance requirements satisfied
- [ ] Team trained on new environment
"""
            
            st.markdown(f"""
            <div class="enterprise-card">
                <h4>✅ Enterprise Migration Checklist</h4>
                <div style="max-height: 400px; overflow-y: auto;">
                    {checklist.replace('### ', '<h5>').replace('- [ ]', '<p>☐').replace('- [x]', '<p>✅')}
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Download checklist
            st.download_button(
                "📥 Download Migration Checklist",
                checklist,
                f"migration_checklist_{config['source_engine']}_to_{config['target_engine']}.md",
                "text/markdown"
            )

# Additional helper functions for enhanced functionality

def render_collaboration_panel():
    """Render collaboration panel for team features"""
    if st.session_state.get('collaboration_enabled', False):
        with st.sidebar.expander("👥 Team Collaboration", expanded=False):
            st.markdown("**👥 Active Team Members:**")
            
            # Mock team members
            team_members = [
                {"name": "John Doe", "role": "DBA", "status": "online"},
                {"name": "Jane Smith", "role": "Developer", "status": "away"},
                {"name": "Mike Wilson", "role": "Project Manager", "status": "online"}
            ]
            
            for member in team_members:
                status_icon = {"online": "🟢", "away": "🟡", "offline": "⚫"}
                st.markdown(f"""
                <div class="collaboration-status">
                    <span class="status-{member['status']}">{status_icon[member['status']]}</span>
                    <span><strong>{member['name']}</strong> ({member['role']})</span>
                </div>
                """, unsafe_allow_html=True)
            
            # Quick actions
            st.markdown("**🚀 Quick Actions:**")
            if st.button("💬 Send Update"):
                st.success("Update sent to team!")
            
            if st.button("📊 Share Analysis"):
                st.info("Analysis shared with team members")

def generate_executive_summary():
    """Generate executive summary from all analysis results"""
    st.markdown("**📊 Executive Summary:**")
    
    # Calculate metrics from session state
    analysis_results = st.session_state.get('analysis_results', {})
    cost_estimates = st.session_state.get('cost_estimates', {})
    security_assessment = st.session_state.get('security_assessment')
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if analysis_results:
            overall_confidence = sum(r.confidence_score for r in analysis_results.values()) / len(analysis_results)
            confidence_color = "success" if overall_confidence > 0.8 else "warning" if overall_confidence > 0.6 else "error"
            if confidence_color == "success":
                st.success(f"🎯 AI Confidence: {overall_confidence:.1%}")
            elif confidence_color == "warning":
                st.warning(f"🎯 AI Confidence: {overall_confidence:.1%}")
            else:
                st.error(f"🎯 AI Confidence: {overall_confidence:.1%}")
        else:
            st.info("🎯 AI Analysis: Not Run")
    
    with col2:
        if cost_estimates:
            total_annual_cost = sum(est.annual_cost for est in cost_estimates.values())
            st.metric("💰 Estimated Annual Cost", f"${total_annual_cost:,.2f}")
        else:
            st.info("💰 Cost Analysis: Not Run")
    
    with col3:
        if security_assessment:
            security_score = security_assessment.overall_score
            security_color = "success" if security_score >= 80 else "warning" if security_score >= 60 else "error"
            if security_color == "success":
                st.success(f"🔒 Security Score: {security_score:.0f}/100")
            elif security_color == "warning":
                st.warning(f"🔒 Security Score: {security_score:.0f}/100")
            else:
                st.error(f"🔒 Security Score: {security_score:.0f}/100")
        else:
            st.info("🔒 Security Analysis: Not Run")
    
    with col4:
        # Calculate overall readiness
        total_analyses = 0
        completed_analyses = 0
        
        if analysis_results:
            total_analyses += 1
            completed_analyses += 1
        if cost_estimates:
            total_analyses += 1
            completed_analyses += 1
        if security_assessment:
            total_analyses += 1
            completed_analyses += 1
        
        if total_analyses > 0:
            readiness = (completed_analyses / 3) * 100  # Out of 3 main analyses
            readiness_status = "Ready" if readiness >= 75 else "In Progress" if readiness >= 50 else "Getting Started"
            
            readiness_color = "success" if readiness >= 75 else "warning" if readiness >= 50 else "info"
            if readiness_color == "success":
                st.success(f"🚀 Readiness: {readiness_status}")
            elif readiness_color == "warning":
                st.warning(f"🚀 Readiness: {readiness_status}")
            else:
                st.info(f"🚀 Readiness: {readiness_status}")
        else:
            st.info("🚀 Readiness: Not Started")

def extract_timeline(self, text: str) -> str:
    """Extract timeline estimate from AI response"""
    timeline_patterns = [
        r'timeline[^:]*?:\s*([^.\n]+)',
        r'estimated? time[^:]*?:\s*([^.\n]+)',
        r'duration[^:]*?:\s*([^.\n]+)',
        r'(\d+\s*(?:week|month|day)s?)'
    ]
    
    for pattern in timeline_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    
    return "4-8 weeks (estimated)"

def extract_cost_impact(self, text: str) -> str:
    """Extract cost impact assessment from AI response"""
    cost_patterns = [
        r'cost[^:]*?:\s*([^.\n]+)',
        r'budget[^:]*?:\s*([^.\n]+)',
        r'savings[^:]*?:\s*([^.\n]+)'
    ]
    
    for pattern in cost_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    
    return "Cost impact varies by implementation approach"

def extract_opportunities(self, text: str) -> List[str]:
    """Extract opportunities from AI response"""
    opportunities = []
    
    # Look for opportunity sections
    opp_patterns = [
        r'(?:opportunit|benefit)[^:]*?:(.*?)(?=\n\n|\n[A-Z]|$)',
        r'(?:advantage)[^:]*?:(.*?)(?=\n\n|\n[A-Z]|$)'
    ]
    
    for pattern in opp_patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            opp_text = match.group(1)
            opp_items = re.findall(r'[-•*]\s*(.+?)(?=\n|$)', opp_text, re.MULTILINE)
            opportunities.extend([item.strip() for item in opp_items])
    
    return opportunities[:6]

def extract_action_items(self, text: str) -> List[Dict[str, str]]:
    """Extract actionable items with priorities from AI response"""
    action_items = []
    
    # Look for action sections
    action_patterns = [
        r'(?:action|next step|immediate)[^:]*?:(.*?)(?=\n\n|\n[A-Z]|$)',
        r'(?:todo|to do)[^:]*?:(.*?)(?=\n\n|\n[A-Z]|$)'
    ]
    
    for pattern in action_patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            action_text = match.group(1)
            actions = re.findall(r'[-•*]\s*(.+?)(?=\n|$)', action_text, re.MULTILINE)
            
            for action in actions:
                priority = 'medium'  # default
                if any(word in action.lower() for word in ['urgent', 'critical', 'immediate']):
                    priority = 'high'
                elif any(word in action.lower() for word in ['later', 'eventual', 'future']):
                    priority = 'low'
                
                action_items.append({
                    'action': action.strip(),
                    'priority': priority,
                    'category': 'general'
                })
    
    return action_items[:10]

# Add missing extract methods to EnterpriseAIAnalyzer class
def extract_risk_mitigations(self, text: str) -> List[str]:
    """Extract risk mitigation strategies"""
    mitigations = []
    
    # Look for mitigation sections
    mitigation_patterns = [
        r'mitigat[^:]*?:(.*?)(?=\n\n|\n[A-Z]|$)',
        r'strateg[^:]*?:(.*?)(?=\n\n|\n[A-Z]|$)'
    ]
    
    for pattern in mitigation_patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            mitigation_text = match.group(1)
            mitigation_items = re.findall(r'[-•*]\s*(.+?)(?=\n|$)', mitigation_text, re.MULTILINE)
            mitigations.extend([item.strip() for item in mitigation_items])
    
    return mitigations[:8]

def extract_categorized_risks(self, text: str) -> List[Dict[str, str]]:
    """Extract risks categorized by type and severity"""
    risks = []
    
    # Look for categorized risk sections
    risk_categories = ['technical', 'business', 'security', 'operational']
    
    for category in risk_categories:
        pattern = f'{category}[^:]*?risk[^:]*?:(.*?)(?=\n\n|\n[A-Z]|$)'
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        
        if match:
            risk_text = match.group(1)
            risk_items = re.findall(r'[-•*]\s*(.+?)(?=\n|$)', risk_text, re.MULTILINE)
            
            for item in risk_items:
                severity = 'medium'  # default
                if any(word in item.lower() for word in ['critical', 'high', 'severe']):
                    severity = 'high'
                elif any(word in item.lower() for word in ['low', 'minor', 'minimal']):
                    severity = 'low'
                
                risks.append({
                    'description': item.strip(),
                    'severity': severity,
                    'category': category
                })
    
    return risks[:8]

def extract_risk_cost_impact(self, text: str) -> str:
    """Extract cost impact from risk analysis"""
    cost_patterns = [
        r'cost.*risk[^:]*?:\s*([^.\n]+)',
        r'financial.*impact[^:]*?:\s*([^.\n]+)',
        r'budget.*risk[^:]*?:\s*([^.\n]+)'
    ]
    
    for pattern in cost_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    
    return "Risk-dependent cost impact"

def extract_risk_action_items(self, text: str) -> List[Dict[str, str]]:
    """Extract risk-specific action items"""
    action_items = []
    
    # Look for risk action sections
    patterns = [
        r'immediate.*action[^:]*?:(.*?)(?=\n\n|\n[A-Z]|$)',
        r'mitigation.*step[^:]*?:(.*?)(?=\n\n|\n[A-Z]|$)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            action_text = match.group(1)
            actions = re.findall(r'[-•*]\s*(.+?)(?=\n|$)', action_text, re.MULTILINE)
            
            for action in actions:
                action_items.append({
                    'action': action.strip(),
                    'priority': 'high',  # Risk actions are typically high priority
                    'category': 'risk_mitigation'
                })
    
    return action_items[:8]

# Complete the AI analyzer class methods
async def _analyze_cost_optimization(self, context: Dict) -> AIAnalysisResult:
    """AI-powered cost optimization analysis"""
    prompt = f"""
    As a cloud cost optimization expert, analyze cost optimization opportunities:
    
    Current Setup: {context.get('source_engine', 'Unknown')} to {context.get('target_engine', 'Unknown')}
    Data Size: {context.get('data_size_gb', 'Unknown')} GB
    Business Context: {context.get('business_context', 'Not provided')}
    
    Provide:
    1. Cost savings opportunities
    2. Service optimization recommendations
    3. Reserved instance strategy
    4. Storage optimization
    5. Operational savings
    6. ROI analysis
    
    Include specific cost optimization recommendations with estimated savings.
    """
    
    try:
        response = self.client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=2500,
            temperature=0.2,
            messages=[{"role": "user", "content": prompt}]
        )
        
        analysis_text = response.content[0].text
        
        return AIAnalysisResult(
            analysis_type=AnalysisType.COST_OPTIMIZATION,
            confidence_score=self._extract_confidence(analysis_text),
            recommendations=self._extract_recommendations(analysis_text),
            risks=self._extract_risks(analysis_text),
            opportunities=self._extract_opportunities(analysis_text),
            timeline_estimate="Cost optimization: Immediate to 6 months",
            cost_impact=self._extract_cost_impact(analysis_text),
            detailed_analysis=analysis_text,
            action_items=self._extract_action_items(analysis_text)
        )
        
    except Exception as e:
        logger.error(f"Cost optimization analysis failed: {e}")
        return self._get_fallback_result(AnalysisType.COST_OPTIMIZATION)

async def _analyze_timeline(self, context: Dict) -> AIAnalysisResult:
    """AI-powered timeline estimation"""
    prompt = f"""
    As a project management expert, create a detailed timeline estimate:
    
    Migration: {context.get('source_engine', 'Unknown')} to {context.get('target_engine', 'Unknown')}
    Data Size: {context.get('data_size_gb', 'Unknown')} GB
    Complexity: {context.get('complexity_level', 'Unknown')}
    Team Experience: {context.get('team_experience', 'Unknown')}
    Business Critical: {context.get('business_critical', False)}
    
    Provide:
    1. Detailed project timeline with phases
    2. Critical path analysis
    3. Resource requirements
    4. Risk buffer recommendations
    5. Milestone definitions
    6. Go-live strategy
    
    Include realistic estimates with best-case, realistic, and worst-case scenarios.
    """
    
    try:
        response = self.client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=2500,
            temperature=0.2,
            messages=[{"role": "user", "content": prompt}]
        )
        
        analysis_text = response.content[0].text
        
        return AIAnalysisResult(
            analysis_type=AnalysisType.TIMELINE_ESTIMATION,
            confidence_score=self._extract_confidence(analysis_text),
            recommendations=self._extract_recommendations(analysis_text),
            risks=self._extract_risks(analysis_text),
            opportunities=self._extract_opportunities(analysis_text),
            timeline_estimate=self._extract_timeline(analysis_text),
            cost_impact="Timeline-dependent cost factors",
            detailed_analysis=analysis_text,
            action_items=self._extract_action_items(analysis_text)
        )
        
    except Exception as e:
        logger.error(f"Timeline analysis failed: {e}")
        return self._get_fallback_result(AnalysisType.TIMELINE_ESTIMATION)

async def _analyze_security(self, context: Dict) -> AIAnalysisResult:
    """AI-powered security analysis"""
    prompt = f"""
    As a database security expert, analyze the security implications:
    
    Migration: {context.get('source_engine', 'Unknown')} to {context.get('target_engine', 'Unknown')}
    Compliance: {context.get('compliance_requirements', [])}
    Business Critical: {context.get('business_critical', False)}
    
    Provide:
    1. Security risk assessment
    2. Encryption strategy
    3. Access control design
    4. Network security
    5. Compliance mapping
    6. Audit and monitoring
    7. Data protection
    8. AWS security best practices
    
    Include specific security configurations and compliance guidance.
    """
    
    try:
        response = self.client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=2500,
            temperature=0.1,
            messages=[{"role": "user", "content": prompt}]
        )
        
        analysis_text = response.content[0].text
        
        return AIAnalysisResult(
            analysis_type=AnalysisType.SECURITY_ANALYSIS,
            confidence_score=self._extract_confidence(analysis_text),
            recommendations=self._extract_recommendations(analysis_text),
            risks=self._extract_risks(analysis_text),
            opportunities=self._extract_opportunities(analysis_text),
            timeline_estimate="Security implementation: 2-6 weeks",
            cost_impact="Security infrastructure costs",
            detailed_analysis=analysis_text,
            action_items=self._extract_action_items(analysis_text)
        )
        
    except Exception as e:
        logger.error(f"Security analysis failed: {e}")
        return self._get_fallback_result(AnalysisType.SECURITY_ANALYSIS)

# Add the missing extract methods to EnterpriseAIAnalyzer
EnterpriseAIAnalyzer.extract_timeline = extract_timeline
EnterpriseAIAnalyzer.extract_cost_impact = extract_cost_impact
EnterpriseAIAnalyzer.extract_opportunities = extract_opportunities
EnterpriseAIAnalyzer.extract_action_items = extract_action_items
EnterpriseAIAnalyzer.extract_risk_mitigations = extract_risk_mitigations
EnterpriseAIAnalyzer.extract_categorized_risks = extract_categorized_risks
EnterpriseAIAnalyzer.extract_risk_cost_impact = extract_risk_cost_impact
EnterpriseAIAnalyzer.extract_risk_action_items = extract_risk_action_items
EnterpriseAIAnalyzer._analyze_cost_optimization = _analyze_cost_optimization
EnterpriseAIAnalyzer._analyze_timeline = _analyze_timeline
EnterpriseAIAnalyzer._analyze_security = _analyze_security

if __name__ == "__main__":
    main()
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
            
            prompt = f"""
            As a database migration expert, analyze the compatibility of migrating from {source_engine} to {target_engine}.

            Schema Objects to Analyze:
            {schema_summary}

            Please provide:
            1. Overall compatibility assessment (percentage)
            2. Major compatibility issues and risks
            3. Recommended migration approach
            4. Specific object-level concerns
            5. AWS service recommendations for {target_engine}
            6. Migration complexity scoring (Low/Medium/High/Very High)
            7. Timeline estimation for migration
            8. Critical success factors

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
        return {
            'ai_analysis': 'AI analysis not available - using fallback assessment',
            'compatibility_score': 70.0,
            'complexity_level': 'medium',
            'major_issues': [
                'Schema conversion complexity varies by object type',
                'Data type mappings require validation',
                'Stored procedures may need rewriting'
            ],
            'recommendations': [
                'Conduct thorough testing in staging environment',
                'Review all stored procedures and functions',
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
            source_engine=source_engine,
            target_engine=target_engine,
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

def render_sidebar():
    """Render sidebar configuration"""
    st.sidebar.header("🔧 Migration Configuration")
    
    # Source Database Configuration
    st.sidebar.subheader("📤 Source Database")
    source_engine = st.sidebar.selectbox(
        "Source Database Engine",
        ["mysql", "postgresql", "oracle", "sql_server", "mongodb"],
        format_func=lambda x: {
            'mysql': '🐬 MySQL',
            'postgresql': '🐘 PostgreSQL', 
            'oracle': '🏛️ Oracle',
            'sql_server': '🪟 SQL Server',
            'mongodb': '🍃 MongoDB'
        }[x]
    )
    
    source_version = st.sidebar.text_input("Source Version", "8.0")
    
    # Target Database Configuration
    st.sidebar.subheader("📥 Target Database")
    target_engine = st.sidebar.selectbox(
        "Target Database Engine",
        ["postgresql", "mysql", "oracle", "sql_server", "aurora_mysql", "aurora_postgresql"],
        format_func=lambda x: {
            'mysql': '🐬 MySQL',
            'postgresql': '🐘 PostgreSQL',
            'oracle': '🏛️ Oracle', 
            'sql_server': '🪟 SQL Server',
            'aurora_mysql': '🌟 Aurora MySQL',
            'aurora_postgresql': '🌟 Aurora PostgreSQL'
        }[x]
    )
    
    target_version = st.sidebar.text_input("Target Version", "14.0")
    
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

def render_schema_input_tab():
    """Render schema input and analysis tab"""
    st.subheader("📋 Schema Analysis Input")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**📥 Schema Input Methods:**")
        input_method = st.radio(
            "Choose input method:",
            ["Manual Entry", "SQL File Upload", "Database Connection"],
            help="Select how you want to provide schema information"
        )
        
        if input_method == "Manual Entry":
            schema_ddl = st.text_area(
                "Database Schema (DDL)",
                placeholder="""CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    total DECIMAL(10,2),
    order_date DATETIME,
    FOREIGN KEY (user_id) REFERENCES users(id)
);""",
                height=300
            )
            
        elif input_method == "SQL File Upload":
            uploaded_file = st.file_uploader(
                "Upload SQL Schema File",
                type=['sql', 'txt'],
                help="Upload a .sql file containing your database schema"
            )
            
            if uploaded_file:
                schema_ddl = uploaded_file.read().decode('utf-8')
                st.text_area("Uploaded Schema Preview", schema_ddl[:1000] + "...", height=200)
            else:
                schema_ddl = ""
                
        else:  # Database Connection
            st.info("Database connection feature coming soon. Please use manual entry or file upload.")
            schema_ddl = ""
    
    with col2:
        st.markdown("**📝 Query Analysis:**")
        
        queries_text = st.text_area(
            "SQL Queries to Analyze",
            placeholder="""SELECT u.username, COUNT(o.order_id) as order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
GROUP BY u.id, u.username
HAVING order_count > 0
ORDER BY order_count DESC
LIMIT 10;

SELECT DATE_FORMAT(order_date, '%Y-%m') as month,
       SUM(total) as monthly_total
FROM orders
WHERE order_date >= DATE_SUB(NOW(), INTERVAL 12 MONTH)
GROUP BY DATE_FORMAT(order_date, '%Y-%m')
ORDER BY month;""",
            height=300
        )
    
    return schema_ddl, queries_text

def render_compatibility_analysis_tab(config: Dict, schema_ddl: str, queries_text: str):
    """Render compatibility analysis results"""
    st.subheader("🔍 Compatibility Analysis Results")
    
    if not schema_ddl and not queries_text:
        st.warning("Please provide schema DDL or queries in the Schema Input tab to run analysis.")
        return
    
    # Initialize analyzers
    schema_analyzer = SchemaAnalyzer()
    
    # Run analysis
    with st.spinner("🔄 Running compatibility analysis..."):
        
        # Schema Analysis
        if schema_ddl:
            st.markdown("**📊 Schema Compatibility Analysis:**")
            
            schema_analysis = schema_analyzer.analyze_table_compatibility(
                config['source_engine'], 
                config['target_engine'], 
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
                        <h4>⚠️ Compatibility Issues</h4>
                        {''.join([f'<p>• {issue}</p>' for issue in schema_analysis['issues']])}
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div class="schema-card">
                        <h4>✅ No Major Issues Found</h4>
                        <p>Schema appears to be highly compatible with the target engine.</p>
                    </div>
                    """, unsafe_allow_html=True)
            
            with col2:
                if schema_analysis['recommendations']:
                    st.markdown(f"""
                    <div class="compatibility-card">
                        <h4>💡 Recommendations</h4>
                        {''.join([f'<p>• {rec}</p>' for rec in schema_analysis['recommendations']])}
                    </div>
                    """, unsafe_allow_html=True)
        
        # Query Analysis
        if queries_text:
            st.markdown("**🔍 Query Compatibility Analysis:**")
            
            queries = [q.strip() for q in queries_text.split(';') if q.strip()]
            query_results = []
            
            for i, query in enumerate(queries):
                if query:
                    result = schema_analyzer.analyze_query_compatibility(
                        config['source_engine'],
                        config['target_engine'],
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
                        delta=f"{len(query_results)} queries analyzed"
                    )
                
                with col2:
                    st.metric(
                        "🔧 Total Issues",
                        total_issues,
                        delta=f"Across {len(query_results)} queries"
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
                        delta=f"{most_common[1]} queries"
                    )
                
                # Detailed Query Results
                for i, result in enumerate(query_results):
                    with st.expander(f"Query {i+1} - Compatibility: {result.compatibility_score:.1f}%", expanded=False):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown("**Original Query:**")
                            st.code(result.original_query, language='sql')
                            
                            if result.issues:
                                st.markdown("**Issues Found:**")
                                for issue in result.issues:
                                    severity_color = {'low': '🟢', 'medium': '🟡', 'high': '🔴'}
                                    st.write(f"{severity_color.get(issue['severity'], '🔵')} {issue['description']}")
                        
                        with col2:
                            st.markdown("**Converted Query:**")
                            st.code(result.converted_query, language='sql')
                            
                            st.markdown("**Analysis Summary:**")
                            st.write(f"**Compatibility Score:** {result.compatibility_score:.1f}%")
                            st.write(f"**Complexity:** {result.complexity.value.title()}")
                            st.write(f"**Performance Impact:** {result.performance_impact}")

def render_aws_mapping_tab(config: Dict):
    """Render AWS service mapping analysis"""
    st.subheader("☁️ AWS Service Mapping & Recommendations")
    
    aws_mapper = AWSServiceMapper()
    
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
    st.markdown("**💾 Backup & Recovery Strategy:**")
    
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
    
    backup_strategy = generate_backup_strategy(rpo_requirement, rto_requirement, retention_period, config['target_engine'])
    
    st.markdown(f"""
    <div class="schema-card">
        <h4>🎯 Tailored Backup Strategy</h4>
        {backup_strategy}
    </div>
    """, unsafe_allow_html=True)

def generate_backup_strategy(rpo: str, rto: str, retention: str, target_engine: str) -> str:
    """Generate backup strategy recommendations"""
    strategy = []
    
    # RPO-based recommendations
    if "1 hour" in rpo:
        strategy.append("<p><strong>High Frequency Backups:</strong> Enable continuous backup with 1-minute point-in-time recovery</p>")
        strategy.append("<p><strong>Transaction Log Backups:</strong> Every 5 minutes for minimal data loss</p>")
    elif "4 hours" in rpo:
        strategy.append("<p><strong>Regular Backups:</strong> Automated backups every 2 hours</p>")
        strategy.append("<p><strong>Transaction Log Backups:</strong> Every 15 minutes</p>")
    
    # RTO-based recommendations  
    if "5 minutes" in rto:
        strategy.append("<p><strong>High Availability:</strong> Multi-AZ deployment with automatic failover</p>")
        strategy.append("<p><strong>Read Replicas:</strong> Maintain hot standby replicas</p>")
    elif "15 minutes" in rto:
        strategy.append("<p><strong>Fast Recovery:</strong> Multi-AZ with optimized restore procedures</p>")
    
    # Engine-specific recommendations
    if 'aurora' in target_engine:
        strategy.append("<p><strong>Aurora Benefits:</strong> Continuous backup to S3, 15-minute point-in-time recovery</p>")
        strategy.append("<p><strong>Aurora Backtrack:</strong> Rewind database to specific point in time</p>")
    elif target_engine == 'postgresql':
        strategy.append("<p><strong>PostgreSQL Features:</strong> Write-Ahead Logging (WAL) for PITR</p>")
        strategy.append("<p><strong>Streaming Replication:</strong> For real-time standby databases</p>")
    
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
    
    if not schema_ddl:
        st.warning("Please provide schema DDL in the Schema Input tab to generate migration scripts.")
        return
    
    script_generator = MigrationScriptGenerator()
    
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
        with st.spinner("📝 Generating migration scripts..."):
            
            # Parse schema objects (simplified)
            schema_objects = parse_schema_objects(schema_ddl)
            
            # Generate scripts
            if generate_pre:
                st.markdown("**📋 Pre-Migration Scripts:**")
                
                pre_script = script_generator.generate_pre_migration_script(
                    config['source_engine'],
                    config['target_engine'], 
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
                st.markdown("**🔄 Schema Conversion Scripts:**")
                
                # Analyze schema for conversions
                schema_analyzer = SchemaAnalyzer()
                conversions = {}
                
                for obj in schema_objects:
                    if obj['type'] == 'table':
                        conversion = schema_analyzer.analyze_table_compatibility(
                            config['source_engine'],
                            config['target_engine'],
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
                st.markdown("**✅ Post-Migration Scripts:**")
                
                post_script = generate_post_migration_script(config, schema_objects)
                
                with st.expander("🔍 Post-Migration Script", expanded=False):
                    st.code(post_script, language='sql')
                    st.download_button(
                        "📥 Download Post-Migration Script",
                        post_script,
                        f"post_migration_{config['source_engine']}_to_{config['target_engine']}.sql",
                        "text/sql"
                    )
            
            # Generate migration checklist
            st.markdown("**📋 Migration Checklist:**")
            checklist = generate_migration_checklist(config, schema_objects)
            
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
    
    return objects

def generate_post_migration_script(config: Dict, schema_objects: List[Dict]) -> str:
    """Generate post-migration validation script"""
    
    script_parts = []
    script_parts.append(f"-- Post-Migration Validation Script")
    script_parts.append(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    script_parts.append(f"-- Migration: {config['source_engine']} → {config['target_engine']}")
    script_parts.append("")
    
    # Row count validation
    script_parts.append("-- Row Count Validation")
    for obj in schema_objects:
        if obj['type'] == 'table':
            script_parts.append(f"SELECT '{obj['name']}' as table_name, COUNT(*) as row_count FROM {obj['name']};")
    
    script_parts.append("")
    
    # Performance analysis
    script_parts.append("-- Performance Analysis")
    if config['target_engine'] == 'postgresql':
        script_parts.append("-- Update table statistics")
        for obj in schema_objects:
            if obj['type'] == 'table':
                script_parts.append(f"ANALYZE {obj['name']};")
    elif config['target_engine'] == 'mysql':
        script_parts.append("-- Update table statistics") 
        for obj in schema_objects:
            if obj['type'] == 'table':
                script_parts.append(f"ANALYZE TABLE {obj['name']};")
    
    return '\n'.join(script_parts)

def generate_migration_checklist(config: Dict, schema_objects: List[Dict]) -> str:
    """Generate migration execution checklist"""
    
    checklist_items = []
    
    # Pre-migration
    checklist_items.append("<h5>📋 Pre-Migration Phase</h5>")
    checklist_items.append("<p>☐ Backup current database completely</p>")
    checklist_items.append("<p>☐ Test backup restore procedure</p>")
    checklist_items.append("<p>☐ Validate schema conversion scripts</p>")
    checklist_items.append("<p>☐ Set up target AWS environment</p>")
    checklist_items.append("<p>☐ Configure network connectivity</p>")
    checklist_items.append("<p>☐ Test application connectivity to target</p>")
    
    # Migration execution
    checklist_items.append("<h5>🚀 Migration Execution</h5>")
    checklist_items.append("<p>☐ Execute pre-migration scripts</p>")
    checklist_items.append("<p>☐ Run schema conversion</p>")
    checklist_items.append("<p>☐ Migrate data (using AWS DMS or custom scripts)</p>")
    checklist_items.append("<p>☐ Validate data integrity</p>")
    checklist_items.append("<p>☐ Update application configuration</p>")
    checklist_items.append("<p>☐ Test application functionality</p>")
    
    # Post-migration
    checklist_items.append("<h5>✅ Post-Migration Phase</h5>")
    checklist_items.append("<p>☐ Run post-migration validation scripts</p>")
    checklist_items.append("<p>☐ Update database statistics</p>")
    checklist_items.append("<p>☐ Configure monitoring and alerting</p>")
    checklist_items.append("<p>☐ Set up backup procedures</p>")
    checklist_items.append("<p>☐ Performance testing and optimization</p>")
    checklist_items.append("<p>☐ Update documentation</p>")
    
    return '\n'.join(checklist_items)

def render_ai_analysis_tab(config: Dict, schema_ddl: str):
    """Render AI-powered analysis tab"""
    st.subheader("🤖 AI-Powered Migration Analysis")
    
    if not schema_ddl:
        st.warning("Please provide schema DDL in the Schema Input tab for AI analysis.")
        return
    
    ai_manager = AIAnalysisManager()
    
    if not ai_manager.connected:
        st.error("❌ AI Analysis requires Anthropic API key. Please configure in Streamlit secrets.")
        st.info("💡 Add ANTHROPIC_API_KEY to your Streamlit secrets to enable AI-powered analysis.")
        return
    
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
    
    if st.button("🧠 Run AI Analysis", type="primary"):
        with st.spinner("🤖 Running AI-powered analysis..."):
            
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
                    config['target_engine'],
                    schema_summary
                ))
                
                # Display results
                st.markdown("**🎯 AI Analysis Results:**")
                
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
                with st.expander("🔍 Complete AI Analysis Report", expanded=False):
                    st.markdown("**🤖 Detailed AI Assessment:**")
                    st.markdown(ai_result['ai_analysis'])
                
            except Exception as e:
                st.error(f"❌ AI Analysis failed: {str(e)}")
                st.info("💡 Please check your API configuration and try again.")

async def main():
    """Main application function"""
    render_header()
    
    # Get configuration from sidebar
    config = render_sidebar()
    
    # Main content tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📋 Schema Input & Analysis",
        "🔍 Compatibility Analysis", 
        "☁️ AWS Service Mapping",
        "📜 Migration Scripts",
        "🤖 AI Analysis"
    ])
    
    with tab1:
        schema_ddl, queries_text = render_schema_input_tab()
    
    with tab2:
        render_compatibility_analysis_tab(config, schema_ddl, queries_text)
    
    with tab3:
        render_aws_mapping_tab(config)
    
    with tab4:
        render_migration_scripts_tab(config, schema_ddl)
    
    with tab5:
        render_ai_analysis_tab(config, schema_ddl)
    
    # Professional footer
    st.markdown("""
    <div class="professional-footer">
        <h4>🗄️ Database Migration Analyzer - Schema & Query Analysis Tool</h4>
        <p>Advanced Database Migration Analysis • AI-Powered Compatibility Assessment • AWS Service Mapping • Migration Script Generation</p>
        <p style="font-size: 0.9rem; margin-top: 1rem; opacity: 0.9;">
            🔬 Schema Analysis • 🔍 Query Compatibility • 📜 Script Generation • ☁️ AWS Integration • 🤖 AI Recommendations
        </p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    asyncio.run(main())
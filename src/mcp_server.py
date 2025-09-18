import sqlite3
import json
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp import types

# Absolute path to database
DB_PATH = 'C:/Users/Noah/Documents/DevSecOps/Project-21-AI-Data-Pipeline/data/pipeline_data.db'

# Initialize MCP server
app = Server("pipeline-inspector")

@app.list_tools()
async def list_tools() -> list[types.Tool]:
    """List available tools"""
    return [
        types.Tool(
            name="query_inspection_data",
            description="Query pipeline inspection data from the sanitised database. Returns anonymised client data for analysis and report generation.",
            inputSchema={
                "type": "object",
                "properties": {
                    "company_id": {
                        "type": "string",
                        "description": "Anonymous company ID (e.g., Company_137)"
                    },
                    "risk_level": {
                        "type": "string",
                        "description": "Filter by risk level: low, medium, high, critical",
                        "enum": ["low", "medium", "high", "critical"]
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of records to return",
                        "default": 10
                    }
                }
            }
        ),
        types.Tool(
            name="get_company_summary",
            description="Get summary statistics for a specific anonymised company",
            inputSchema={
                "type": "object",
                "properties": {
                    "company_id": {
                        "type": "string",
                        "description": "Anonymous company ID (e.g., Company_137)"
                    }
                },
                "required": ["company_id"]
            }
        ),
        types.Tool(
            name="list_companies",
            description="List all anonymised company IDs in the database",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        )
    ]

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    """Handle tool calls"""
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        if name == "query_inspection_data":
            company_id = arguments.get("company_id")
            risk_level = arguments.get("risk_level")
            limit = arguments.get("limit", 10)
            
            query = "SELECT * FROM inspection_data WHERE 1=1"
            params = []
            
            if company_id:
                query += " AND client_name = ?"
                params.append(company_id)
            
            if risk_level:
                query += " AND risk_level = ?"
                params.append(risk_level)
            
            query += f" LIMIT {limit}"
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            
            results = [dict(zip(columns, row)) for row in rows]
            
            return [types.TextContent(
                type="text",
                text=json.dumps(results, indent=2)
            )]
        
        elif name == "get_company_summary":
            company_id = arguments.get("company_id")
            
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_inspections,
                    AVG(metal_loss_percent) as avg_metal_loss,
                    COUNT(CASE WHEN risk_level = 'critical' THEN 1 END) as critical_count,
                    COUNT(CASE WHEN risk_level = 'high' THEN 1 END) as high_count,
                    COUNT(CASE WHEN defect_type != 'none' THEN 1 END) as defects_found
                FROM inspection_data
                WHERE client_name = ?
            """, (company_id,))
            
            result = cursor.fetchone()
            columns = [description[0] for description in cursor.description]
            summary = dict(zip(columns, result))
            
            return [types.TextContent(
                type="text",
                text=json.dumps(summary, indent=2)
            )]
        
        elif name == "list_companies":
            cursor.execute("SELECT DISTINCT client_name FROM inspection_data")
            companies = [row[0] for row in cursor.fetchall()]
            
            return [types.TextContent(
                type="text",
                text=json.dumps({"companies": companies}, indent=2)
            )]
    
    finally:
        conn.close()

async def main():
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
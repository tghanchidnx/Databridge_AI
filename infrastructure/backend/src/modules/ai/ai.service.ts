import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

@Injectable()
export class AiService {
  private readonly logger = new Logger(AiService.name);
  private readonly openaiApiKey: string;

  constructor(private configService: ConfigService) {
    this.openaiApiKey = this.configService.get<string>('OPENAI_API_KEY');
  }

  /**
   * Process chat message and generate response
   */
  async processChat(
    message: string,
    context?: Array<{ role: string; content: string }>,
  ): Promise<{ response: string; suggestions?: string[] }> {
    try {
      // Build conversation context
      const messages = [
        {
          role: 'system',
          content: `You are a helpful database management assistant for Data Amplifier. 
You help with:
- SQL query writing and optimization
- Database connections (Snowflake, MySQL, PostgreSQL, etc.)
- Schema comparisons and migrations
- Report matching and analysis
- GitHub integration
- Troubleshooting connection issues

Provide clear, concise answers with code examples when relevant.`,
        },
        ...(context || []),
        { role: 'user', content: message },
      ];

      // If OpenAI is configured, use it
      if (this.openaiApiKey && this.openaiApiKey !== 'your-openai-api-key') {
        return await this.callOpenAI(messages);
      }

      // Fallback to rule-based responses
      return this.getFallbackResponse(message);
    } catch (error) {
      this.logger.error('Chat processing failed', error.stack);
      return this.getFallbackResponse(message);
    }
  }

  /**
   * Generate SQL query from natural language
   */
  async generateSql(
    query: string,
    databaseType?: string,
    tableSchemas?: string[],
  ): Promise<{ sql: string; explanation: string }> {
    try {
      const prompt = `Generate a ${databaseType || 'SQL'} query for: ${query}${
        tableSchemas?.length ? `\n\nAvailable tables:\n${tableSchemas.join('\n')}` : ''
      }`;

      if (this.openaiApiKey && this.openaiApiKey !== 'your-openai-api-key') {
        const response = await this.callOpenAI([
          {
            role: 'system',
            content:
              'You are a SQL expert. Generate valid SQL queries based on user requests. Return only the SQL query and a brief explanation.',
          },
          { role: 'user', content: prompt },
        ]);

        return {
          sql: this.extractSqlFromResponse(response.response),
          explanation: response.response,
        };
      }

      // Fallback SQL templates
      return this.generateFallbackSql(query, databaseType);
    } catch (error) {
      this.logger.error('SQL generation failed', error.stack);
      throw error;
    }
  }

  /**
   * Explain SQL query
   */
  async explainQuery(query: string): Promise<{ explanation: string }> {
    try {
      if (this.openaiApiKey && this.openaiApiKey !== 'your-openai-api-key') {
        const response = await this.callOpenAI([
          {
            role: 'system',
            content: 'You are a SQL expert. Explain SQL queries in simple terms.',
          },
          { role: 'user', content: `Explain this SQL query:\n\n${query}` },
        ]);

        return { explanation: response.response };
      }

      return { explanation: this.getFallbackQueryExplanation(query) };
    } catch (error) {
      this.logger.error('Query explanation failed', error.stack);
      throw error;
    }
  }

  /**
   * Call OpenAI API
   */
  private async callOpenAI(
    messages: Array<{ role: string; content: string }>,
  ): Promise<{ response: string; suggestions?: string[] }> {
    try {
      const response = await fetch('https://api.openai.com/v1/chat/completions', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${this.openaiApiKey}`,
        },
        body: JSON.stringify({
          model: 'gpt-4o-mini',
          messages,
          temperature: 0.7,
          max_tokens: 1000,
        }),
      });

      if (!response.ok) {
        throw new Error(`OpenAI API error: ${response.statusText}`);
      }

      const data = await response.json();
      return {
        response: data.choices[0].message.content,
      };
    } catch (error) {
      this.logger.error('OpenAI API call failed', error.stack);
      throw error;
    }
  }

  /**
   * Fallback response when AI is not available
   */
  private getFallbackResponse(query: string): { response: string; suggestions?: string[] } {
    const lowercaseQuery = query.toLowerCase();

    if (lowercaseQuery.includes('snowflake') || lowercaseQuery.includes('connect')) {
      return {
        response: `To connect to Snowflake:

1. Navigate to **Connections** page
2. Click **New Connection**
3. Select **Snowflake** as database type
4. Fill in connection details:
   - Account identifier (e.g., myaccount.us-east-1)
   - Warehouse name
   - Database and schema
   - Authentication method

5. Test connection and save

Would you like help with a specific authentication method?`,
        suggestions: [
          'How to use OAuth with Snowflake?',
          'Setup key-pair authentication',
          'Test Snowflake connection',
        ],
      };
    }

    if (lowercaseQuery.includes('query') || lowercaseQuery.includes('sql')) {
      return {
        response: `I can help you write SQL queries! Here's an example:

\`\`\`sql
SELECT 
  c.customer_name,
  COUNT(o.order_id) as total_orders,
  SUM(o.amount) as total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_date >= CURRENT_DATE - 30
GROUP BY c.customer_name
ORDER BY total_spent DESC;
\`\`\`

What specific query would you like to create?`,
        suggestions: ['Write a JOIN query', 'Optimize my query', 'Explain query performance'],
      };
    }

    if (lowercaseQuery.includes('schema') || lowercaseQuery.includes('comparison')) {
      return {
        response: `Schema comparison features:

**Compare schemas between:**
- Two Snowflake databases
- Two MySQL/PostgreSQL databases
- Cross-platform (Snowflake ↔ MySQL)

**Generates:**
- Detailed difference reports
- DDL deployment scripts
- Dependency-ordered migrations
- Rollback scripts

Ready to run a comparison?`,
        suggestions: [
          'Run schema comparison',
          'Generate deployment script',
          'View comparison history',
        ],
      };
    }

    return {
      response: `I can help you with:

• **Database Connections** - Setup Snowflake, MySQL, PostgreSQL
• **SQL Queries** - Write, optimize, and explain queries
• **Schema Comparisons** - Compare and migrate schemas
• **Integrations** - GitHub, CI/CD pipelines

What would you like to accomplish?`,
      suggestions: [
        'Connect to database',
        'Write SQL query',
        'Compare schemas',
        'Setup integration',
      ],
    };
  }

  /**
   * Extract SQL from AI response
   */
  private extractSqlFromResponse(response: string): string {
    const sqlMatch = response.match(/```sql\n([\s\S]+?)\n```/);
    if (sqlMatch) {
      return sqlMatch[1].trim();
    }
    return response.trim();
  }

  /**
   * Generate fallback SQL
   */
  private generateFallbackSql(
    query: string,
    databaseType?: string,
  ): { sql: string; explanation: string } {
    const lowercaseQuery = query.toLowerCase();

    if (lowercaseQuery.includes('customer') && lowercaseQuery.includes('order')) {
      return {
        sql: `SELECT 
  c.customer_id,
  c.customer_name,
  COUNT(o.order_id) as total_orders,
  SUM(o.amount) as total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name
ORDER BY total_spent DESC;`,
        explanation:
          'This query joins customers with their orders and calculates total orders and spending per customer.',
      };
    }

    return {
      sql: 'SELECT * FROM your_table LIMIT 10;',
      explanation:
        'Basic SELECT query template. Please provide more details about your requirements.',
    };
  }

  /**
   * Fallback query explanation
   */
  private getFallbackQueryExplanation(query: string): string {
    const parts = [];

    if (query.toUpperCase().includes('SELECT')) {
      parts.push('**SELECT**: Retrieves data from database');
    }
    if (query.toUpperCase().includes('JOIN')) {
      parts.push('**JOIN**: Combines rows from multiple tables');
    }
    if (query.toUpperCase().includes('WHERE')) {
      parts.push('**WHERE**: Filters results based on conditions');
    }
    if (query.toUpperCase().includes('GROUP BY')) {
      parts.push('**GROUP BY**: Groups rows with same values');
    }
    if (query.toUpperCase().includes('ORDER BY')) {
      parts.push('**ORDER BY**: Sorts results');
    }

    return parts.length > 0
      ? `This query:\n\n${parts.join('\n')}`
      : 'This appears to be a SQL query. Please provide more context for detailed explanation.';
  }
}

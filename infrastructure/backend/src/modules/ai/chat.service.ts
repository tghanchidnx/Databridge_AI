/**
 * Hierarchy Chat Service
 * Context-aware AI chat for hierarchy building with function calling
 */
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

export interface ChatMessage {
  role: 'user' | 'assistant' | 'system';
  content: string;
  timestamp?: Date;
  functionCall?: {
    name: string;
    arguments: Record<string, any>;
  };
}

export interface HierarchyChange {
  type: 'create' | 'update' | 'delete' | 'move' | 'rename';
  hierarchyId: string;
  hierarchyName?: string;
  field?: string;
  oldValue?: any;
  newValue?: any;
  description: string;
}

export interface ChatContext {
  projectId?: string;
  projectName?: string;
  currentHierarchyId?: string;
  currentHierarchyName?: string;
  hierarchyPath?: string[];
  hierarchies?: any[]; // Full hierarchy data for AI context
  recentActions?: Array<{
    action: string;
    timestamp: Date;
    details: string;
  }>;
  projectStats?: {
    totalHierarchies: number;
    totalMappings: number;
    unmappedCount: number;
  };
  conversationHistory?: Array<{ role: string; content: string }>;
}

export interface ChatResponse {
  message: string;
  response?: string; // Alias for message
  suggestions?: string[];
  changes?: HierarchyChange[]; // Structured changes to apply
  functionCall?: {
    name: string;
    arguments: Record<string, any>;
    requiresConfirmation: boolean;
  };
  context?: Partial<ChatContext>;
}

// Available functions the AI can call
const AVAILABLE_FUNCTIONS = [
  {
    name: 'navigateToHierarchy',
    description: 'Navigate to a specific hierarchy node by ID or name',
    parameters: {
      type: 'object',
      properties: {
        hierarchyId: { type: 'string', description: 'The hierarchy ID to navigate to' },
        hierarchyName: { type: 'string', description: 'The hierarchy name to search for' },
      },
    },
  },
  {
    name: 'createHierarchy',
    description: 'Create a new hierarchy node',
    parameters: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Name of the new hierarchy' },
        parentId: { type: 'string', description: 'Parent hierarchy ID' },
        description: { type: 'string', description: 'Optional description' },
      },
      required: ['name'],
    },
  },
  {
    name: 'updateMapping',
    description: 'Update or add a mapping to a hierarchy node',
    parameters: {
      type: 'object',
      properties: {
        hierarchyId: { type: 'string', description: 'The hierarchy to update' },
        sourceDatabase: { type: 'string' },
        sourceSchema: { type: 'string' },
        sourceTable: { type: 'string' },
        sourceColumn: { type: 'string' },
      },
      required: ['hierarchyId', 'sourceTable', 'sourceColumn'],
    },
  },
  {
    name: 'suggestFormula',
    description: 'Get formula suggestions for a hierarchy node',
    parameters: {
      type: 'object',
      properties: {
        hierarchyId: { type: 'string', description: 'The hierarchy to get formula suggestions for' },
      },
      required: ['hierarchyId'],
    },
  },
  {
    name: 'runValidation',
    description: 'Run validation checks on the project or specific hierarchy',
    parameters: {
      type: 'object',
      properties: {
        scope: { type: 'string', enum: ['project', 'hierarchy'], description: 'Validation scope' },
        hierarchyId: { type: 'string', description: 'Hierarchy ID if scope is hierarchy' },
      },
    },
  },
  {
    name: 'exportHierarchy',
    description: 'Export hierarchy data',
    parameters: {
      type: 'object',
      properties: {
        format: { type: 'string', enum: ['csv', 'json', 'sql'], description: 'Export format' },
        includeChildren: { type: 'boolean', description: 'Include child hierarchies' },
      },
    },
  },
  {
    name: 'findUnmappedNodes',
    description: 'Find hierarchy nodes without mappings',
    parameters: {
      type: 'object',
      properties: {
        parentId: { type: 'string', description: 'Optional parent to search under' },
      },
    },
  },
  {
    name: 'searchHierarchies',
    description: 'Search for hierarchies by name or pattern',
    parameters: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'Search query' },
        includeDescendants: { type: 'boolean' },
      },
      required: ['query'],
    },
  },
];

@Injectable()
export class HierarchyChatService {
  private readonly logger = new Logger(HierarchyChatService.name);
  private readonly anthropicApiKey: string;
  private readonly openaiApiKey: string;
  private conversationHistory = new Map<string, ChatMessage[]>();

  constructor(private configService: ConfigService) {
    this.anthropicApiKey = this.configService.get<string>('ANTHROPIC_API_KEY') || '';
    this.openaiApiKey = this.configService.get<string>('OPENAI_API_KEY') || '';
  }

  /**
   * Process a chat message with context
   */
  async processMessage(
    sessionId: string,
    message: string,
    context: ChatContext,
  ): Promise<ChatResponse> {
    try {
      // Get or create conversation history
      const history = this.conversationHistory.get(sessionId) || [];

      // Add user message to history
      history.push({
        role: 'user',
        content: message,
        timestamp: new Date(),
      });

      // Build system prompt with context
      const systemPrompt = this.buildSystemPrompt(context);

      let response: ChatResponse;

      if (this.anthropicApiKey) {
        response = await this.callClaude(systemPrompt, history, context);
      } else if (this.openaiApiKey) {
        response = await this.callOpenAI(systemPrompt, history, context);
      } else {
        response = this.getFallbackResponse(message, context);
      }

      // Add assistant response to history
      history.push({
        role: 'assistant',
        content: response.message,
        timestamp: new Date(),
        functionCall: response.functionCall ? {
          name: response.functionCall.name,
          arguments: response.functionCall.arguments,
        } : undefined,
      });

      // Keep only last 20 messages
      if (history.length > 20) {
        history.splice(0, history.length - 20);
      }

      this.conversationHistory.set(sessionId, history);

      return response;
    } catch (error) {
      this.logger.error('Chat processing failed', error);
      return {
        message: 'I encountered an error processing your request. Please try again.',
        suggestions: ['Show project status', 'Find unmapped nodes', 'Help with mappings'],
      };
    }
  }

  /**
   * Clear conversation history for a session
   */
  clearHistory(sessionId: string): void {
    this.conversationHistory.delete(sessionId);
  }

  /**
   * Build system prompt with context
   */
  private buildSystemPrompt(context: ChatContext): string {
    const parts = [
      `You are an AI assistant for the Hierarchy Knowledge Base Builder application.
You help users build, manage, and validate financial hierarchies for reporting systems.

CRITICAL INSTRUCTION - YOU MUST FOLLOW THIS EXACTLY:
When the user asks you to make ANY changes to hierarchies (move, rename, create, delete, update), you MUST:
1. Write a brief explanation (one sentence)
2. IMMEDIATELY include a JSON code block with the changes array

YOU MUST ALWAYS INCLUDE THE JSON BLOCK. Never say you will do something without providing the JSON.

ABSOLUTE REQUIREMENT: The "hierarchyName" field is REQUIRED in EVERY change object. It is used for lookup.

Example - Moving a hierarchy (use REAL IDs from the list below):
User: "Move Daily throughput under Gross Volume"
Response: "I'll move 'Daily throughput' under 'Gross Volume' for you.

\`\`\`json
[
  {
    "type": "move",
    "hierarchyId": "abc123-real-id-from-list",
    "hierarchyName": "Daily throughput",
    "newValue": "xyz789-parent-real-id",
    "description": "Moved Daily throughput under Gross Volume"
  }
]
\`\`\`"

Example - Renaming a hierarchy:
User: "Rename Revenue to Total Revenue"
Response: "I'll rename 'Revenue' to 'Total Revenue'.

\`\`\`json
[
  {
    "type": "rename",
    "hierarchyId": "real-id-from-list",
    "hierarchyName": "Revenue",
    "field": "hierarchyName",
    "oldValue": "Revenue",
    "newValue": "Total Revenue",
    "description": "Renamed Revenue to Total Revenue"
  }
]
\`\`\`"

Example - Changing sort order (IMPORTANT: "move X up/down" in sort order means updating sortOrder, NOT changing parent):
User: "Move Gross Volume one sort down"
Response: "I'll move 'Gross Volume' down one position in the sort order.

\`\`\`json
[
  {
    "type": "update",
    "hierarchyId": "real-id-from-list",
    "hierarchyName": "Gross Volume",
    "field": "sortOrder",
    "oldValue": 5,
    "newValue": 6,
    "description": "Moved Gross Volume down one position (sortOrder 5 -> 6)"
  }
]
\`\`\`"

Note: When user says "move X up/down one" or "move X one sort up/down", they mean change the sortOrder number.
- "move up" = decrease sortOrder (e.g., 5 -> 4)
- "move down" = increase sortOrder (e.g., 5 -> 6)
This is DIFFERENT from "move X under Y" which changes the parent.

Change types you can use:
- "create": Create a new hierarchy (provide: hierarchyName, newValue=parentId or null for root)
- "update": Update a field (provide: hierarchyId, hierarchyName, field, oldValue, newValue)
- "delete": Delete a hierarchy (provide: hierarchyId, hierarchyName)
- "move": Move to new parent (provide: hierarchyId, hierarchyName, newValue=new parentId)
- "rename": Rename hierarchy (provide: hierarchyId, hierarchyName, oldValue, newValue)

Fields you can update:
- hierarchyName: The display name
- description: The description text
- flags.include_flag, flags.exclude_flag, flags.active_flag: Boolean flags
- sortOrder: Numeric sort order

### CRITICAL ID LOOKUP - READ THIS CAREFULLY ###

1. SEARCH the "HIERARCHY LOOKUP TABLE" below to find the EXACT ID for each hierarchy name mentioned by the user
2. The "hierarchyName" field is REQUIRED and used as a backup if ID lookup fails - ALWAYS include it
3. NEVER use placeholder IDs like "id-of-something", "daily-throughput-id", "abc123", etc.
4. Use case-insensitive matching - "daily throughput" matches "Daily Throughput"
5. If you cannot find an exact match, use a partial match (e.g., "daily" matches "Daily Throughput Volume")
6. Copy the EXACT ID string from the lookup table into your JSON
7. If no match exists at all, tell the user: "I couldn't find '[name]'. Available: [list 3 similar names]"`,
    ];

    // Build a clear lookup table for hierarchies
    if (context.hierarchies?.length) {
      const maxEntries = 300; // Increased from 100 to show more hierarchies
      parts.push(`\n### HIERARCHY LOOKUP TABLE (showing ${Math.min(context.hierarchies.length, maxEntries)} of ${context.hierarchies.length} entries) ###`);
      parts.push(`Use these EXACT IDs in your JSON responses:\n`);

      // Sort alphabetically to make it easier to find entries
      const sortedHierarchies = [...context.hierarchies]
        .sort((a, b) => (a.hierarchyName || '').localeCompare(b.hierarchyName || ''));

      const hierarchyTable = sortedHierarchies.slice(0, maxEntries).map(h => {
        const id = h.id || h.hierarchyId;
        const name = h.hierarchyName || 'Unnamed';
        const parent = h.parentId ? `(parent: ${h.parentId})` : '(ROOT)';
        const sortOrder = h.sortOrder !== undefined ? `[sort:${h.sortOrder}]` : '';
        return `"${name}" => ID: "${id}" ${parent} ${sortOrder}`;
      }).join('\n');
      parts.push(hierarchyTable);
      if (context.hierarchies.length > maxEntries) {
        parts.push(`\n... and ${context.hierarchies.length - maxEntries} more hierarchies not shown`);
      }
      parts.push(`\n### END LOOKUP TABLE ###`);

      // Add search hints
      parts.push(`\n### SEARCH TIPS ###`);
      parts.push(`- If user asks about a name not found, try partial matching (e.g., "Rev" might match "Revenue" or "Total Revenue")`);
      parts.push(`- Names are case-insensitive for matching`);
      parts.push(`- List available similar names if you can't find an exact match`);
    }

    if (context.projectName) {
      parts.push(`\nCurrent Project: ${context.projectName} (ID: ${context.projectId})`);
    }

    if (context.currentHierarchyName) {
      parts.push(`Currently selected: "${context.currentHierarchyName}" (ID: ${context.currentHierarchyId})`);
    }

    if (context.hierarchyPath?.length) {
      parts.push(`Path: ${context.hierarchyPath.join(' > ')}`);
    }

    if (context.projectStats) {
      parts.push(`\nProject Stats:
- Total Hierarchies: ${context.projectStats.totalHierarchies}
- Total Mappings: ${context.projectStats.totalMappings}
- Unmapped Nodes: ${context.projectStats.unmappedCount}`);
    }

    if (context.recentActions?.length) {
      parts.push(`\nRecent Actions:`);
      context.recentActions.slice(0, 5).forEach(action => {
        parts.push(`- ${action.action}: ${action.details}`);
      });
    }

    return parts.join('\n');
  }

  /**
   * Parse changes from AI response
   */
  private parseChangesFromResponse(text: string): { message: string; changes: HierarchyChange[] } {
    const jsonMatch = text.match(/```json\n?([\s\S]*?)\n?```/);
    let changes: HierarchyChange[] = [];
    let message = text;

    if (jsonMatch) {
      try {
        const parsed = JSON.parse(jsonMatch[1]);
        if (Array.isArray(parsed)) {
          changes = parsed;
        } else if (parsed.changes && Array.isArray(parsed.changes)) {
          changes = parsed.changes;
        }
        // Remove the JSON block from the message for cleaner display
        message = text.replace(/```json\n?[\s\S]*?\n?```/, '').trim();
      } catch (e) {
        this.logger.warn('Failed to parse AI changes JSON:', e);
      }
    }

    return { message, changes };
  }

  /**
   * Call Claude API
   */
  private async callClaude(
    systemPrompt: string,
    history: ChatMessage[],
    context: ChatContext,
  ): Promise<ChatResponse> {
    const messages = history.map(m => ({
      role: m.role as 'user' | 'assistant',
      content: m.content,
    }));

    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': this.anthropicApiKey,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 2048,
        system: systemPrompt,
        messages,
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      this.logger.error(`Claude API error: ${response.status} - ${errorText}`);
      throw new Error(`Claude API error: ${response.statusText}`);
    }

    const data = await response.json();

    // Parse response - handle text content
    const content = data.content[0];
    const responseText = content.type === 'text' ? content.text : JSON.stringify(content);

    this.logger.log(`Claude response text: ${responseText.substring(0, 500)}...`);

    // Parse for JSON changes block
    const { message, changes } = this.parseChangesFromResponse(responseText);

    this.logger.log(`Parsed ${changes.length} changes from response`);
    if (changes.length > 0) {
      this.logger.log(`Changes: ${JSON.stringify(changes)}`);
    }

    return {
      message,
      response: message,
      changes: changes.length > 0 ? changes : undefined,
      suggestions: this.extractSuggestions(message, context),
    };
  }

  /**
   * Call OpenAI API with function calling
   */
  private async callOpenAI(
    systemPrompt: string,
    history: ChatMessage[],
    context: ChatContext,
  ): Promise<ChatResponse> {
    const messages = [
      { role: 'system', content: systemPrompt },
      ...history.map(m => ({
        role: m.role,
        content: m.content,
      })),
    ];

    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${this.openaiApiKey}`,
      },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        messages,
        functions: AVAILABLE_FUNCTIONS,
        function_call: 'auto',
        temperature: 0.7,
      }),
    });

    if (!response.ok) {
      throw new Error(`OpenAI API error: ${response.statusText}`);
    }

    const data = await response.json();
    const choice = data.choices[0];

    if (choice.message.function_call) {
      const fnCall = choice.message.function_call;
      return {
        message: `I'll ${fnCall.name.replace(/([A-Z])/g, ' $1').toLowerCase()} for you.`,
        functionCall: {
          name: fnCall.name,
          arguments: JSON.parse(fnCall.arguments),
          requiresConfirmation: this.needsConfirmation(fnCall.name),
        },
        suggestions: this.getSuggestionsForAction(fnCall.name),
      };
    }

    return {
      message: choice.message.content,
      suggestions: this.extractSuggestions(choice.message.content, context),
    };
  }

  /**
   * Check if action needs confirmation
   */
  private needsConfirmation(actionName: string): boolean {
    const confirmationRequired = [
      'createHierarchy',
      'updateMapping',
      'exportHierarchy',
    ];
    return confirmationRequired.includes(actionName);
  }

  /**
   * Get suggestions for specific action
   */
  private getSuggestionsForAction(actionName: string): string[] {
    const suggestions: Record<string, string[]> = {
      navigateToHierarchy: ['View mapping details', 'Edit hierarchy', 'Add child node'],
      createHierarchy: ['Add mapping', 'Create child', 'Set formula'],
      updateMapping: ['Validate mapping', 'View source data', 'Find similar mappings'],
      suggestFormula: ['Accept suggestion', 'View alternatives', 'Edit manually'],
      runValidation: ['Fix issues', 'Export report', 'Ignore warnings'],
      exportHierarchy: ['Include all children', 'Filter by status', 'Schedule export'],
      findUnmappedNodes: ['Map all', 'Get suggestions', 'Export list'],
      searchHierarchies: ['Refine search', 'Filter results', 'Export matches'],
    };
    return suggestions[actionName] || [];
  }

  /**
   * Extract suggestions from response
   */
  private extractSuggestions(response: string, context: ChatContext): string[] {
    const suggestions: string[] = [];

    if (context.projectStats?.unmappedCount && context.projectStats.unmappedCount > 0) {
      suggestions.push('Find unmapped nodes');
    }

    if (context.currentHierarchyId) {
      suggestions.push('View mapping details');
      suggestions.push('Suggest formula');
    }

    if (!context.currentHierarchyId) {
      suggestions.push('Navigate to hierarchy');
      suggestions.push('Show project overview');
    }

    return suggestions.slice(0, 4);
  }

  /**
   * Fallback response when AI is not available
   */
  private getFallbackResponse(message: string, context: ChatContext): ChatResponse {
    const lowerMessage = message.toLowerCase();

    if (lowerMessage.includes('unmapped') || lowerMessage.includes('missing')) {
      return {
        message: `I'll find the unmapped nodes in your project.`,
        functionCall: {
          name: 'findUnmappedNodes',
          arguments: {},
          requiresConfirmation: false,
        },
        suggestions: ['Get mapping suggestions', 'Export unmapped list', 'View by level'],
      };
    }

    if (lowerMessage.includes('navigate') || lowerMessage.includes('go to')) {
      const match = message.match(/(?:to|called|named)\s+['""]?([^'""\n]+)['""]?/i);
      if (match) {
        return {
          message: `Navigating to "${match[1]}"...`,
          functionCall: {
            name: 'navigateToHierarchy',
            arguments: { hierarchyName: match[1] },
            requiresConfirmation: false,
          },
        };
      }
    }

    if (lowerMessage.includes('validate') || lowerMessage.includes('check')) {
      return {
        message: 'Running validation on your project...',
        functionCall: {
          name: 'runValidation',
          arguments: { scope: 'project' },
          requiresConfirmation: false,
        },
      };
    }

    if (lowerMessage.includes('export')) {
      return {
        message: 'What format would you like to export?',
        suggestions: ['Export as CSV', 'Export as JSON', 'Export as SQL'],
      };
    }

    // Default helpful response
    return {
      message: `I can help you with your hierarchy project "${context.projectName || 'Unknown'}". Here's what I can do:

• **Navigate** - Go to a specific hierarchy ("go to Revenue")
• **Create** - Add new hierarchy nodes ("create a child called Expenses")
• **Map** - Set up source mappings ("map this to sales_data.amount")
• **Validate** - Check for issues ("validate my project")
• **Export** - Export data ("export as CSV")
• **Find** - Search or find unmapped nodes

What would you like to do?`,
      suggestions: [
        'Find unmapped nodes',
        'Run validation',
        'Show project stats',
        'Export hierarchy',
      ],
    };
  }
}

import React, { useState, useRef, useEffect, useCallback, useImperativeHandle, forwardRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import {
  Undo2,
  Redo2,
  Send,
  Bot,
  User,
  Loader2,
  Save,
  Sparkles,
  AlertCircle,
  CheckCircle2,
  Settings,
  Key,
  ExternalLink,
  Copy,
  CheckCheck,
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { addLog } from '@/components/dev-console';

export interface HierarchyChange {
  type: 'create' | 'update' | 'delete' | 'move' | 'rename';
  hierarchyId: string;
  hierarchyName?: string;
  field?: string;
  oldValue?: any;
  newValue?: any;
  description: string;
}

export interface ChatMessage {
  id: string;
  role: 'user' | 'assistant' | 'system';
  content: string;
  timestamp: Date;
  changes?: HierarchyChange[];
  isApplied?: boolean;
  isError?: boolean;
}

interface AIChatProps {
  hierarchies: any[];
  projectId: string;
  projectName?: string;
  selectedHierarchy?: any;
  onApplyChanges: (changes: HierarchyChange[]) => Promise<void>;
  onUndo: () => void;
  onRedo: () => void;
  canUndo: boolean;
  canRedo: boolean;
  undoDescription: string | null;
  redoDescription: string | null;
  onSaveAndCommit: () => Promise<void>;
  hasUnsavedChanges: boolean;
}

// Ref handle type for programmatic control
export interface AIChatRef {
  sendMessage: (message: string) => void;
}

// Setup Guide Component for when API key is missing
const SetupGuide: React.FC<{ onDismiss: () => void }> = ({ onDismiss }) => {
  const navigate = useNavigate();
  const [copied, setCopied] = useState(false);

  const copyEnvExample = () => {
    navigator.clipboard.writeText('ANTHROPIC_API_KEY=sk-ant-your-key-here');
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="p-4 space-y-4">
      <div className="flex items-center gap-2 text-amber-600 dark:text-amber-400">
        <Key className="h-5 w-5" />
        <span className="font-semibold">API Key Required</span>
      </div>

      <p className="text-sm text-muted-foreground">
        To use the AI Assistant, you need to configure an Anthropic (Claude) API key.
      </p>

      <div className="bg-muted/50 rounded-lg p-4 space-y-3">
        <h4 className="font-medium text-sm">Setup Instructions:</h4>

        <div className="space-y-2 text-sm">
          <div className="flex gap-2">
            <span className="bg-primary text-primary-foreground rounded-full w-5 h-5 flex items-center justify-center text-xs flex-shrink-0">1</span>
            <div>
              <p>Get an API key from Anthropic:</p>
              <a
                href="https://console.anthropic.com/settings/keys"
                target="_blank"
                rel="noopener noreferrer"
                className="text-blue-600 hover:underline flex items-center gap-1 mt-1"
              >
                console.anthropic.com/settings/keys
                <ExternalLink className="h-3 w-3" />
              </a>
            </div>
          </div>

          <div className="flex gap-2">
            <span className="bg-primary text-primary-foreground rounded-full w-5 h-5 flex items-center justify-center text-xs flex-shrink-0">2</span>
            <div>
              <p>Add to your environment file:</p>
              <div className="bg-background rounded border mt-1 p-2 font-mono text-xs flex items-center justify-between">
                <code>ANTHROPIC_API_KEY=sk-ant-...</code>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-6 w-6 p-0"
                  onClick={copyEnvExample}
                >
                  {copied ? <CheckCheck className="h-3 w-3 text-green-500" /> : <Copy className="h-3 w-3" />}
                </Button>
              </div>
            </div>
          </div>

          <div className="flex gap-2">
            <span className="bg-primary text-primary-foreground rounded-full w-5 h-5 flex items-center justify-center text-xs flex-shrink-0">3</span>
            <p>Restart the backend service to apply changes</p>
          </div>
        </div>
      </div>

      <div className="bg-blue-50 dark:bg-blue-950/30 rounded-lg p-3 border border-blue-200 dark:border-blue-800">
        <p className="text-sm text-blue-800 dark:text-blue-200">
          <strong>File location:</strong><br/>
          <code className="text-xs bg-blue-100 dark:bg-blue-900 px-1 rounded">v2/.env</code> - line 97
        </p>
      </div>

      <div className="flex gap-2">
        <Button
          variant="outline"
          size="sm"
          className="flex-1"
          onClick={() => navigate('/ai-config')}
        >
          <Settings className="h-4 w-4 mr-1" />
          AI Settings
        </Button>
        <Button
          variant="ghost"
          size="sm"
          onClick={onDismiss}
        >
          Dismiss
        </Button>
      </div>
    </div>
  );
};

export const AIChat = forwardRef<AIChatRef, AIChatProps>(({
  hierarchies,
  projectId,
  projectName,
  selectedHierarchy,
  onApplyChanges,
  onUndo,
  onRedo,
  canUndo,
  canRedo,
  undoDescription,
  redoDescription,
  onSaveAndCommit,
  hasUnsavedChanges,
}, ref) => {
  const [messages, setMessages] = useState<ChatMessage[]>([
    {
      id: 'welcome',
      role: 'assistant',
      content: `Hello! I'm your AI assistant for managing hierarchies. I can help you:

- **Create** new hierarchies or nodes
- **Rename** existing hierarchies
- **Move** nodes to different parents
- **Update** hierarchy properties (flags, mappings, levels)
- **Delete** hierarchies

Just describe what changes you'd like to make in natural language. Changes are saved automatically.`,
      timestamp: new Date(),
    },
  ]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [showSetupGuide, setShowSetupGuide] = useState(false);
  const [apiKeyMissing, setApiKeyMissing] = useState(false);
  const [showAutocomplete, setShowAutocomplete] = useState(false);
  const [autocompleteQuery, setAutocompleteQuery] = useState('');
  const [autocompleteIndex, setAutocompleteIndex] = useState(0);
  const [slashPosition, setSlashPosition] = useState(-1);
  const scrollRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const autocompleteRef = useRef<HTMLDivElement>(null);

  // Get filtered hierarchies for autocomplete
  const filteredHierarchies = React.useMemo(() => {
    if (!showAutocomplete || !hierarchies) return [];
    const query = autocompleteQuery.toLowerCase();
    return hierarchies
      .filter(h => h.hierarchyName?.toLowerCase().includes(query))
      .slice(0, 10); // Limit to 10 results
  }, [hierarchies, autocompleteQuery, showAutocomplete]);

  // Auto-scroll to bottom on new messages
  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  // Focus input on mount and when loading completes
  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  // Refocus when a hierarchy is selected
  useEffect(() => {
    inputRef.current?.focus();
  }, [selectedHierarchy]);

  const parseAIResponse = (response: string): { message: string; changes: HierarchyChange[] } => {
    // Extract JSON changes from the response if present
    const jsonMatch = response.match(/```json\n?([\s\S]*?)\n?```/);
    let changes: HierarchyChange[] = [];
    let message = response;

    // Log if we found a JSON block
    if (jsonMatch) {
      addLog({
        type: 'ai',
        category: 'JSON Parse',
        message: `Found JSON block, attempting to parse`,
        details: { jsonContent: jsonMatch[1] },
      });

      try {
        const parsed = JSON.parse(jsonMatch[1]);
        if (Array.isArray(parsed)) {
          changes = parsed;
        } else if (parsed.changes && Array.isArray(parsed.changes)) {
          changes = parsed.changes;
        }
        // Remove the JSON block from the message
        message = response.replace(/```json\n?[\s\S]*?\n?```/, '').trim();
      } catch (e) {
        addLog({
          type: 'error',
          category: 'JSON Parse Error',
          message: `Failed to parse JSON: ${e}`,
          details: { jsonContent: jsonMatch[1], error: e },
        });
        console.error('Failed to parse AI changes:', e);
      }
    } else {
      // Check if there's any code block at all
      const anyCodeBlock = response.match(/```(\w*)\n?([\s\S]*?)\n?```/);
      if (anyCodeBlock) {
        addLog({
          type: 'warning',
          category: 'Non-JSON Block',
          message: `Found ${anyCodeBlock[1] || 'unmarked'} code block instead of JSON`,
          details: { blockType: anyCodeBlock[1], content: anyCodeBlock[2] },
        });
      } else {
        addLog({
          type: 'warning',
          category: 'No JSON Block',
          message: `AI response did not contain a JSON code block`,
          details: { responsePreview: response.substring(0, 300) },
        });
      }
    }

    return { message, changes };
  };

  const sendMessage = async (externalMessage?: string) => {
    const messageToSend = externalMessage || input.trim();
    if (!messageToSend || isLoading) return;

    const userMessage: ChatMessage = {
      id: `user-${Date.now()}`,
      role: 'user',
      content: messageToSend,
      timestamp: new Date(),
    };

    setMessages((prev) => [...prev, userMessage]);
    if (!externalMessage) setInput('');
    setIsLoading(true);

    addLog({
      type: 'ai',
      category: 'AI Request',
      message: `Sending message to AI: "${messageToSend.substring(0, 100)}${messageToSend.length > 100 ? '...' : ''}"`,
    });

    try {
      // Prepare hierarchy context for the AI
      const hierarchyContext = hierarchies.map((h) => ({
        id: h.id,
        hierarchyId: h.hierarchyId,
        hierarchyName: h.hierarchyName,
        parentId: h.parentId,
        isRoot: h.isRoot,
        hierarchyLevel: h.hierarchyLevel,
        sortOrder: h.sortOrder,
        flags: h.flags,
        mapping: h.mapping,
      }));

      // Log hierarchy context for debugging
      const hierarchyNames = hierarchyContext.map(h => h.hierarchyName).filter(Boolean);
      addLog({
        type: 'ai',
        category: 'Context Sent',
        message: `Sending ${hierarchyContext.length} hierarchies to AI for project "${projectName}" (${projectId})`,
        details: {
          sampleNames: hierarchyNames.slice(0, 20),
          totalCount: hierarchyContext.length
        },
      });

      const token = localStorage.getItem('auth_token');
      if (!token) {
        throw new Error('Not authenticated');
      }

      // Generate a session ID based on project
      const sessionId = `viewer-${projectId}`;

      const response = await fetch('/api/ai/hierarchy-chat', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({
          sessionId,
          message: messageToSend,
          context: {
            projectId,
            projectName,
            currentHierarchyId: selectedHierarchy?.id || selectedHierarchy?.hierarchyId,
            currentHierarchyName: selectedHierarchy?.hierarchyName,
            hierarchies: hierarchyContext,
            conversationHistory: messages.slice(-10).map((m) => ({
              role: m.role,
              content: m.content,
            })),
          },
        }),
      });

      if (!response.ok) {
        throw new Error('Failed to get AI response');
      }

      const responseData = await response.json();
      // Backend wraps response in {data: {...}, statusCode, message, timestamp}
      const data = responseData.data || responseData;
      const rawResponse = data.response || data.message || responseData.message || '';

      // Log to dev console with full response content
      addLog({
        type: 'ai',
        category: 'AI Response',
        message: `AI returned: "${rawResponse.substring(0, 200)}${rawResponse.length > 200 ? '...' : ''}"`,
        details: { fullResponse: rawResponse, data, responseData },
      });

      // Use changes from backend if already parsed, otherwise parse from response text
      let aiMessage = rawResponse;
      let changes: HierarchyChange[] = [];

      if (data.changes && Array.isArray(data.changes) && data.changes.length > 0) {
        // Backend already parsed changes - use them directly
        changes = data.changes;
        aiMessage = rawResponse;
        addLog({
          type: 'ai',
          category: 'Backend Changes',
          message: `Using ${changes.length} change(s) from backend`,
          details: changes,
        });
      } else {
        // Fallback: try to parse from response text (in case backend didn't parse)
        const parsed = parseAIResponse(rawResponse);
        aiMessage = parsed.message;
        changes = parsed.changes;
      }

      addLog({
        type: 'ai',
        category: 'AI Parsing',
        message: `Parsed ${changes.length} change(s) from response`,
        details: changes,
      });

      const assistantMessage: ChatMessage = {
        id: `assistant-${Date.now()}`,
        role: 'assistant',
        content: aiMessage,
        timestamp: new Date(),
        changes: changes.length > 0 ? changes : undefined,
        isApplied: false,
      };

      setMessages((prev) => [...prev, assistantMessage]);

      // Auto-apply changes if present
      if (changes.length > 0) {
        addLog({
          type: 'change',
          category: 'Applying Changes',
          message: `Attempting to apply ${changes.length} change(s)`,
          details: changes,
        });

        try {
          await onApplyChanges(changes);
          setMessages((prev) =>
            prev.map((m) =>
              m.id === assistantMessage.id ? { ...m, isApplied: true } : m
            )
          );

          addLog({
            type: 'change',
            category: 'Changes Applied',
            message: `Successfully applied ${changes.length} change(s)`,
            details: changes.map(c => `${c.type}: ${c.hierarchyName || c.hierarchyId}`),
          });
        } catch (error: any) {
          addLog({
            type: 'error',
            category: 'Apply Failed',
            message: `Failed to apply changes: ${error?.message || 'Unknown error'}`,
            details: { error, changes },
          });

          setMessages((prev) =>
            prev.map((m) =>
              m.id === assistantMessage.id
                ? { ...m, isError: true, content: m.content + '\n\n⚠️ Failed to apply some changes.' }
                : m
            )
          );
        }
      }
    } catch (error: any) {
      addLog({
        type: 'error',
        category: 'AI Error',
        message: `AI Chat error: ${error?.message || 'Unknown error'}`,
        details: error,
      });

      // Check if this is an API key error
      const errorStr = error?.message || '';
      const isApiKeyError = errorStr.includes('API') || errorStr.includes('key') || errorStr.includes('authentication');

      if (isApiKeyError || !apiKeyMissing) {
        // Check backend response for API key issues
        setApiKeyMissing(true);
        setShowSetupGuide(true);
      }

      const errorMessage: ChatMessage = {
        id: `error-${Date.now()}`,
        role: 'assistant',
        content: apiKeyMissing
          ? '🔑 **API Key Required**\n\nThe AI assistant needs an Anthropic API key to work. Please check the setup guide above to configure it.'
          : 'Sorry, I encountered an error processing your request. Please try again.',
        timestamp: new Date(),
        isError: true,
      };
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
      inputRef.current?.focus();
    }
  };

  // Expose sendMessage to parent via ref
  useImperativeHandle(ref, () => ({
    sendMessage: (message: string) => {
      sendMessage(message);
    },
  }), [sendMessage]);

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setInput(value);

    // Check for "/" to trigger autocomplete
    const cursorPos = e.target.selectionStart || 0;
    const textBeforeCursor = value.slice(0, cursorPos);
    const lastSlashIndex = textBeforeCursor.lastIndexOf('/');

    if (lastSlashIndex !== -1) {
      // Check if there's a space before the slash (or it's at the start)
      const charBeforeSlash = lastSlashIndex > 0 ? textBeforeCursor[lastSlashIndex - 1] : ' ';
      if (charBeforeSlash === ' ' || lastSlashIndex === 0) {
        const query = textBeforeCursor.slice(lastSlashIndex + 1);
        // Only show autocomplete if no space after the query started
        if (!query.includes(' ')) {
          setShowAutocomplete(true);
          setAutocompleteQuery(query);
          setSlashPosition(lastSlashIndex);
          setAutocompleteIndex(0);
          return;
        }
      }
    }

    setShowAutocomplete(false);
    setAutocompleteQuery('');
    setSlashPosition(-1);
  };

  const selectAutocompleteItem = (hierarchyName: string) => {
    if (slashPosition === -1) return;

    // Replace the /query with the selected hierarchy name
    const beforeSlash = input.slice(0, slashPosition);
    const cursorPos = inputRef.current?.selectionStart || input.length;
    const afterQuery = input.slice(cursorPos);

    const newInput = `${beforeSlash}"${hierarchyName}"${afterQuery}`;
    setInput(newInput);
    setShowAutocomplete(false);
    setAutocompleteQuery('');
    setSlashPosition(-1);

    // Focus back on input
    setTimeout(() => {
      inputRef.current?.focus();
      const newCursorPos = beforeSlash.length + hierarchyName.length + 2; // +2 for quotes
      inputRef.current?.setSelectionRange(newCursorPos, newCursorPos);
    }, 0);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    // Handle autocomplete navigation
    if (showAutocomplete && filteredHierarchies.length > 0) {
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        setAutocompleteIndex(i => Math.min(i + 1, filteredHierarchies.length - 1));
        return;
      }
      if (e.key === 'ArrowUp') {
        e.preventDefault();
        setAutocompleteIndex(i => Math.max(i - 1, 0));
        return;
      }
      if (e.key === 'Enter' || e.key === 'Tab') {
        e.preventDefault();
        selectAutocompleteItem(filteredHierarchies[autocompleteIndex].hierarchyName);
        return;
      }
      if (e.key === 'Escape') {
        e.preventDefault();
        setShowAutocomplete(false);
        return;
      }
    }

    // Send message on Enter
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  };

  const handleSaveAndCommit = async () => {
    setIsSaving(true);
    try {
      await onSaveAndCommit();
      const systemMessage: ChatMessage = {
        id: `system-${Date.now()}`,
        role: 'system',
        content: '✅ Changes have been saved and committed to the database.',
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, systemMessage]);
    } catch (error) {
      console.error('Save error:', error);
      const errorMessage: ChatMessage = {
        id: `error-${Date.now()}`,
        role: 'system',
        content: '❌ Failed to save changes. Please try again.',
        timestamp: new Date(),
        isError: true,
      };
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsSaving(false);
    }
  };

  const handleUndoClick = () => {
    onUndo();
    const systemMessage: ChatMessage = {
      id: `system-${Date.now()}`,
      role: 'system',
      content: `↩️ Undid: ${undoDescription || 'last change'}`,
      timestamp: new Date(),
    };
    setMessages((prev) => [...prev, systemMessage]);
  };

  const handleRedoClick = () => {
    onRedo();
    const systemMessage: ChatMessage = {
      id: `system-${Date.now()}`,
      role: 'system',
      content: `↪️ Redid: ${redoDescription || 'last undone change'}`,
      timestamp: new Date(),
    };
    setMessages((prev) => [...prev, systemMessage]);
  };

  return (
    <Card className="flex flex-col h-full border-l shadow-lg bg-background overflow-hidden">
      {/* Header with Undo/Redo - Fixed at top */}
      <CardHeader className="flex-shrink-0 p-3 border-b bg-gradient-to-r from-purple-50 to-blue-50 dark:from-purple-950/30 dark:to-blue-950/30">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Sparkles className="h-5 w-5 text-purple-600" />
            <CardTitle className="text-base font-semibold">AI Assistant</CardTitle>
          </div>
          <div className="flex items-center gap-1">
            {/* Undo Button */}
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={handleUndoClick}
                    disabled={!canUndo}
                    className={cn(
                      "h-8 w-8 p-0",
                      canUndo && "text-blue-600 hover:text-blue-700 hover:bg-blue-50"
                    )}
                  >
                    <Undo2 className="h-4 w-4" />
                  </Button>
                </TooltipTrigger>
                <TooltipContent>
                  {canUndo ? `Undo: ${undoDescription}` : 'Nothing to undo'}
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>

            {/* Redo Button */}
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={handleRedoClick}
                    disabled={!canRedo}
                    className={cn(
                      "h-8 w-8 p-0",
                      canRedo && "text-blue-600 hover:text-blue-700 hover:bg-blue-50"
                    )}
                  >
                    <Redo2 className="h-4 w-4" />
                  </Button>
                </TooltipTrigger>
                <TooltipContent>
                  {canRedo ? `Redo: ${redoDescription}` : 'Nothing to redo'}
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>

          </div>
        </div>

        {/* Save & Commit Button */}
        {hasUnsavedChanges && (
          <Button
            onClick={handleSaveAndCommit}
            disabled={isSaving}
            className="mt-2 w-full bg-gradient-to-r from-green-600 to-emerald-600 hover:from-green-700 hover:to-emerald-700"
            size="sm"
          >
            {isSaving ? (
              <>
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                Saving...
              </>
            ) : (
              <>
                <Save className="h-4 w-4 mr-2" />
                Save & Commit Changes
              </>
            )}
          </Button>
        )}
      </CardHeader>

      {/* Messages Area - Scrollable */}
      <div className="flex-1 overflow-y-auto min-h-0" ref={scrollRef}>
        <div className="p-3 space-y-4">
          {/* Setup Guide when API key is missing */}
          {showSetupGuide && (
            <SetupGuide onDismiss={() => setShowSetupGuide(false)} />
          )}

          {messages.map((message) => (
            <div
              key={message.id}
              className={cn(
                "flex gap-2",
                message.role === 'user' ? 'flex-row-reverse' : 'flex-row'
              )}
            >
              {/* Avatar */}
              <div
                className={cn(
                  "flex-shrink-0 h-8 w-8 rounded-full flex items-center justify-center",
                  message.role === 'user'
                    ? 'bg-blue-600 text-white'
                    : message.role === 'system'
                    ? 'bg-gray-400 text-white'
                    : 'bg-gradient-to-r from-purple-600 to-blue-600 text-white'
                )}
              >
                {message.role === 'user' ? (
                  <User className="h-4 w-4" />
                ) : message.role === 'system' ? (
                  <AlertCircle className="h-4 w-4" />
                ) : (
                  <Bot className="h-4 w-4" />
                )}
              </div>

              {/* Message Content */}
              <div
                className={cn(
                  "flex flex-col max-w-[85%] rounded-lg px-3 py-2",
                  message.role === 'user'
                    ? 'bg-blue-600 text-white'
                    : message.role === 'system'
                    ? 'bg-gray-100 dark:bg-gray-800 text-gray-700 dark:text-gray-300'
                    : message.isError
                    ? 'bg-red-50 dark:bg-red-950/30 text-red-800 dark:text-red-200 border border-red-200'
                    : 'bg-gray-100 dark:bg-gray-800'
                )}
              >
                <div className="text-sm whitespace-pre-wrap">{message.content}</div>

                {/* Changes Applied Indicator */}
                {message.changes && message.changes.length > 0 && (
                  <div className="mt-2 pt-2 border-t border-gray-200 dark:border-gray-700">
                    <div className="flex items-center gap-1 text-xs">
                      {message.isApplied ? (
                        <>
                          <CheckCircle2 className="h-3 w-3 text-green-600" />
                          <span className="text-green-600">
                            {message.changes.length} change(s) applied
                          </span>
                        </>
                      ) : message.isError ? (
                        <>
                          <AlertCircle className="h-3 w-3 text-red-600" />
                          <span className="text-red-600">Failed to apply</span>
                        </>
                      ) : (
                        <>
                          <Loader2 className="h-3 w-3 animate-spin" />
                          <span>Applying changes...</span>
                        </>
                      )}
                    </div>
                    <div className="mt-1 space-y-1">
                      {message.changes.map((change, idx) => (
                        <Badge
                          key={idx}
                          variant="outline"
                          className="text-xs mr-1"
                        >
                          {change.type}: {change.hierarchyName || change.hierarchyId}
                        </Badge>
                      ))}
                    </div>
                  </div>
                )}

                {/* Timestamp */}
                <div className="text-xs opacity-50 mt-1">
                  {message.timestamp.toLocaleTimeString()}
                </div>
              </div>
            </div>
          ))}

          {/* Loading Indicator */}
          {isLoading && (
            <div className="flex gap-2">
              <div className="flex-shrink-0 h-8 w-8 rounded-full bg-gradient-to-r from-purple-600 to-blue-600 flex items-center justify-center text-white">
                <Bot className="h-4 w-4" />
              </div>
              <div className="bg-gray-100 dark:bg-gray-800 rounded-lg px-3 py-2">
                <div className="flex items-center gap-2">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  <span className="text-sm">Thinking...</span>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Input Area - Fixed at bottom */}
      <div className="flex-shrink-0 p-3 border-t bg-gray-50 dark:bg-gray-900/50 relative">
        {/* Autocomplete Dropdown */}
        {showAutocomplete && filteredHierarchies.length > 0 && (
          <div
            ref={autocompleteRef}
            className="absolute bottom-full left-0 right-0 mx-3 mb-1 bg-white dark:bg-gray-800 border rounded-lg shadow-lg max-h-48 overflow-y-auto z-50"
          >
            <div className="p-1">
              <div className="text-xs text-muted-foreground px-2 py-1 border-b">
                Type to filter hierarchies (↑↓ to navigate, Enter to select)
              </div>
              {filteredHierarchies.map((h, idx) => (
                <div
                  key={h.id || h.hierarchyId}
                  className={cn(
                    "px-3 py-2 cursor-pointer rounded text-sm flex items-center gap-2",
                    idx === autocompleteIndex
                      ? "bg-blue-100 dark:bg-blue-900"
                      : "hover:bg-gray-100 dark:hover:bg-gray-700"
                  )}
                  onClick={() => selectAutocompleteItem(h.hierarchyName)}
                >
                  <span className="truncate flex-1">{h.hierarchyName}</span>
                  <span className="text-xs text-muted-foreground truncate max-w-[100px]">
                    {h.hierarchyId}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}
        <div className="flex gap-2 relative">
          <Input
            ref={inputRef}
            value={input}
            onChange={handleInputChange}
            onKeyDown={handleKeyDown}
            placeholder="Type / to search hierarchies..."
            disabled={isLoading}
            className="flex-1"
          />
          <Button
            onClick={() => sendMessage()}
            disabled={!input.trim() || isLoading}
            className="bg-gradient-to-r from-purple-600 to-blue-600 hover:from-purple-700 hover:to-blue-700"
          >
            {isLoading ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Send className="h-4 w-4" />
            )}
          </Button>
        </div>
        <p className="text-xs text-muted-foreground mt-2">
          Press Enter to send. Changes are saved automatically.
        </p>
      </div>
    </Card>
  );
});

// Add display name for debugging
AIChat.displayName = 'AIChat';

export default AIChat;

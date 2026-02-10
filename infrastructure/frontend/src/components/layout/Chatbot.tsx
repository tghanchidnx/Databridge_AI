import { useState, useRef, useEffect } from "react";
import {
  X,
  PaperPlaneTilt,
  Sparkle,
  Code,
  Database,
  FileText,
} from "@phosphor-icons/react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { toast } from "sonner";

interface Message {
  id: string;
  role: "user" | "assistant" | "system";
  content: string;
  timestamp: Date;
  suggestions?: string[];
}

interface ChatbotProps {
  onClose: () => void;
}

export function Chatbot({ onClose }: ChatbotProps) {
  const [messages, setMessages] = useState<Message[]>([
    {
      id: "1",
      role: "assistant",
      content:
        "Hello! I'm your Data Amplifier assistant. I can help you with:\n\n• Writing and optimizing SQL queries\n• Understanding database schemas\n• Troubleshooting connection issues\n• Explaining comparison results\n• Setting up integrations\n\nHow can I assist you today?",
      timestamp: new Date(),
      suggestions: [
        "How do I connect to Snowflake?",
        "Help me write a JOIN query",
        "Explain schema comparison results",
        "Setup GitHub integration",
      ],
    },
  ]);
  const [input, setInput] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [openaiApiKey, setOpenaiApiKey] = useState<string>("");
  const scrollAreaRef = useRef<HTMLDivElement>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const stored = localStorage.getItem("openai_api_key");
    if (stored) setOpenaiApiKey(stored);
  }, []);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const callOpenAI = async (userQuery: string): Promise<string> => {
    // Use backend AI API
    try {
      const { apiService } = await import("@/lib/api-service");
      const result = await apiService.sendChatMessage(userQuery);
      return result.response;
    } catch (error) {
      console.error("Backend AI Error:", error);
      return getFallbackResponse(userQuery);
    }
  };

  const getFallbackResponse = (query: string): string => {
    const lowercaseQuery = query.toLowerCase();

    if (
      lowercaseQuery.includes("snowflake") ||
      lowercaseQuery.includes("connect")
    ) {
      return "To connect to Snowflake:\n\n1. Go to **Connections** in the sidebar\n2. Click **New Connection**\n3. Select **Snowflake** as the database type\n4. Fill in:\n   - Account identifier (e.g., myaccount.us-east-1)\n   - Warehouse name\n   - Database and schema\n   - Authentication method (Username/Password, Key Pair, or OAuth)\n\n5. Click **Test Connection** to verify\n6. Save the connection\n\nNeed help with a specific authentication method?";
    }

    if (
      lowercaseQuery.includes("query") ||
      lowercaseQuery.includes("sql") ||
      lowercaseQuery.includes("join")
    ) {
      return "I can help you write SQL queries! Here's a basic JOIN example:\n\n```sql\nSELECT \n  c.customer_name,\n  o.order_id,\n  o.order_date\nFROM customers c\nINNER JOIN orders o\n  ON c.customer_id = o.customer_id\nWHERE o.order_date >= CURRENT_DATE - 30;\n```\n\nThis query:\n- Joins customers and orders tables\n- Shows customer names with their orders\n- Filters for orders in the last 30 days\n\nWhat specific query do you need help with?";
    }

    if (
      lowercaseQuery.includes("github") ||
      lowercaseQuery.includes("integration")
    ) {
      return "To setup GitHub integration:\n\n1. Go to **Settings** → **Integrations**\n2. Click **Connect GitHub**\n3. Authorize Data Amplifier to access your repos\n4. Select which repositories to connect\n5. Choose default branch for version control\n\nOnce connected, you can:\n- Store comparison configurations\n- Track schema changes\n- Commit deployment scripts\n- Create pull requests for approvals\n\nWould you like help with a specific integration feature?";
    }

    if (
      lowercaseQuery.includes("schema") ||
      lowercaseQuery.includes("comparison")
    ) {
      return "Schema comparison helps you:\n\n**Find Differences:**\n- Tables, views, procedures, functions\n- Column types and constraints\n- Indexes and foreign keys\n\n**Generate Scripts:**\n- Automatic DDL generation\n- Dependency-aware ordering\n- Safe migration strategies\n\n**Track Changes:**\n- Version control integration\n- Change history\n- Rollback capabilities\n\nWant to run your first comparison?";
    }

    return `I understand you're asking about: "${query}"\n\nI can provide specific guidance on:\n• Database connections (Snowflake, PostgreSQL, MySQL, etc.)\n• SQL query writing and optimization\n• Schema comparisons and migrations\n• Report matching and analysis\n• GitHub and version control integration\n• Workspace and team management\n\nCould you provide more details about what you'd like to accomplish?\n\n**Note:** Configure your OpenAI API key in Settings for enhanced AI-powered responses!`;
  };

  const handleSend = async () => {
    if (!input.trim() || isLoading) return;

    const userMessage: Message = {
      id: Date.now().toString(),
      role: "user",
      content: input,
      timestamp: new Date(),
    };

    setMessages((prev) => [...prev, userMessage]);
    const currentInput = input;
    setInput("");
    setIsLoading(true);

    try {
      const aiResponse = await callOpenAI(currentInput);

      const assistantMessage: Message = {
        id: (Date.now() + 1).toString(),
        role: "assistant",
        content: aiResponse,
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, assistantMessage]);
    } catch (error) {
      console.error("Error getting AI response:", error);
      const errorMessage: Message = {
        id: (Date.now() + 1).toString(),
        role: "assistant",
        content:
          "I apologize, but I encountered an error. Please try again or configure your OpenAI API key in Settings.",
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleSuggestionClick = (suggestion: string) => {
    setInput(suggestion);
  };

  const getIcon = (messageContent: string) => {
    if (messageContent.includes("```sql") || messageContent.includes("query"))
      return Code;
    if (
      messageContent.includes("database") ||
      messageContent.includes("Snowflake")
    )
      return Database;
    if (
      messageContent.includes("schema") ||
      messageContent.includes("comparison")
    )
      return FileText;
    return Sparkle;
  };

  return (
    <Card className="fixed bottom-4 right-4 w-[calc(100vw-2rem)] sm:w-96 h-[600px] max-h-[calc(100vh-2rem)] shadow-xl border flex flex-col bg-card z-50 rounded-lg overflow-hidden">
      <div className="flex items-center justify-between p-4 border-b bg-muted/30 shrink-0">
        <div className="flex items-center gap-2">
          <div className="w-8 h-8 rounded-full bg-primary/20 flex items-center justify-center">
            <Sparkle className="w-4 h-4 text-primary" weight="fill" />
          </div>
          <h3 className="font-semibold text-foreground">AI Assistant</h3>
        </div>
        <Button
          variant="ghost"
          size="icon"
          onClick={onClose}
          className="h-8 w-8 hover:bg-muted"
        >
          <X className="w-4 h-4" />
        </Button>
      </div>

      <ScrollArea className="flex-1 overflow-y-auto" ref={scrollAreaRef}>
        <div className="p-4 space-y-4 pb-4">
          {messages.map((message) => {
            const Icon =
              message.role === "assistant" ? getIcon(message.content) : null;

            return (
              <div
                key={message.id}
                className={cn(
                  "flex gap-3",
                  message.role === "user" ? "justify-end" : "justify-start"
                )}
              >
                {message.role === "assistant" && Icon && (
                  <div className="w-8 h-8 rounded-full bg-primary/20 flex items-center justify-center shrink-0 mt-0.5">
                    <Icon className="w-4 h-4 text-primary" />
                  </div>
                )}

                <div
                  className={cn(
                    "max-w-[80%] rounded-lg p-3 text-sm",
                    message.role === "user"
                      ? "bg-primary text-primary-foreground"
                      : "bg-muted text-foreground border"
                  )}
                >
                  <div className="whitespace-pre-wrap leading-relaxed wrap-break-word">
                    {message.content.split("```").map((part, idx) => {
                      if (idx % 2 === 1) {
                        const lines = part.split("\n");
                        const lang = lines[0];
                        const code = lines.slice(1).join("\n");
                        return (
                          <pre
                            key={idx}
                            className="bg-card/80 rounded p-2 my-2 overflow-x-auto border"
                          >
                            <code className="text-xs font-mono">{code}</code>
                          </pre>
                        );
                      }
                      return (
                        <span
                          key={idx}
                          dangerouslySetInnerHTML={{
                            __html: part.replace(
                              /\*\*(.*?)\*\*/g,
                              "<strong>$1</strong>"
                            ),
                          }}
                        />
                      );
                    })}
                  </div>

                  {message.suggestions && (
                    <div className="mt-3 flex flex-wrap gap-2">
                      {message.suggestions.map((suggestion, idx) => (
                        <Badge
                          key={idx}
                          variant="secondary"
                          className="cursor-pointer hover:bg-primary hover:text-primary-foreground transition-colors text-xs"
                          onClick={() => handleSuggestionClick(suggestion)}
                        >
                          {suggestion}
                        </Badge>
                      ))}
                    </div>
                  )}

                  <p className="text-xs opacity-70 mt-2">
                    {message.timestamp.toLocaleTimeString()}
                  </p>
                </div>
              </div>
            );
          })}

          {isLoading && (
            <div className="flex gap-3">
              <div className="w-8 h-8 rounded-full bg-primary/20 flex items-center justify-center mt-0.5">
                <Sparkle className="w-4 h-4 text-primary animate-pulse" />
              </div>
              <div className="bg-muted rounded-lg p-3 border">
                <div className="flex gap-1">
                  <div
                    className="w-2 h-2 bg-muted-foreground rounded-full animate-bounce"
                    style={{ animationDelay: "0ms" }}
                  />
                  <div
                    className="w-2 h-2 bg-muted-foreground rounded-full animate-bounce"
                    style={{ animationDelay: "150ms" }}
                  />
                  <div
                    className="w-2 h-2 bg-muted-foreground rounded-full animate-bounce"
                    style={{ animationDelay: "300ms" }}
                  />
                </div>
              </div>
            </div>
          )}
          <div ref={messagesEndRef} />
        </div>
      </ScrollArea>

      <div className="p-4 border-t bg-background shrink-0">
        <div className="flex gap-2">
          <Input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && handleSend()}
            placeholder="Ask me anything..."
            disabled={isLoading}
            className="flex-1 bg-background border-input"
          />
          <Button
            onClick={handleSend}
            disabled={!input.trim() || isLoading}
            size="icon"
            className="shrink-0"
          >
            <PaperPlaneTilt className="w-4 h-4" weight="fill" />
          </Button>
        </div>
        {!openaiApiKey && (
          <p className="text-xs text-muted-foreground mt-2">
            Configure OpenAI API key in Settings for enhanced AI
          </p>
        )}
      </div>
    </Card>
  );
}

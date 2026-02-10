import React, { useState, useEffect, useRef, useCallback } from 'react';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import {
  ChevronUp,
  ChevronDown,
  Terminal,
  Brain,
  FileText,
  AlertTriangle,
  Trash2,
  Download,
  Copy,
  CheckCheck,
  X,
  Maximize2,
  Minimize2,
} from 'lucide-react';
import { cn } from '@/lib/utils';

export interface LogEntry {
  id: string;
  timestamp: Date;
  type: 'info' | 'warning' | 'error' | 'ai' | 'api' | 'change';
  category: string;
  message: string;
  details?: any;
}

interface DevConsoleProps {
  className?: string;
}

// Global log store
let globalLogs: LogEntry[] = [];
let logListeners: ((logs: LogEntry[]) => void)[] = [];

export const addLog = (entry: Omit<LogEntry, 'id' | 'timestamp'>) => {
  const newEntry: LogEntry = {
    ...entry,
    id: `log-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
    timestamp: new Date(),
  };
  globalLogs = [...globalLogs.slice(-500), newEntry]; // Keep last 500 logs
  logListeners.forEach(listener => listener(globalLogs));
};

export const clearLogs = (category?: string) => {
  if (category) {
    globalLogs = globalLogs.filter(log => {
      if (category === 'ai') return log.type !== 'ai';
      if (category === 'errors') return log.type !== 'error' && log.type !== 'warning';
      if (category === 'audit') return log.type !== 'change';
      return true;
    });
  } else {
    globalLogs = [];
  }
  logListeners.forEach(listener => listener(globalLogs));
};

// Hook to subscribe to logs
const useLogs = () => {
  const [logs, setLogs] = useState<LogEntry[]>(globalLogs);

  useEffect(() => {
    const listener = (newLogs: LogEntry[]) => setLogs([...newLogs]);
    logListeners.push(listener);
    return () => {
      logListeners = logListeners.filter(l => l !== listener);
    };
  }, []);

  return logs;
};

// Intercept console methods
const originalConsole = {
  log: console.log,
  warn: console.warn,
  error: console.error,
  info: console.info,
};

let consoleIntercepted = false;

export const interceptConsole = () => {
  if (consoleIntercepted) return;
  consoleIntercepted = true;

  console.log = (...args) => {
    originalConsole.log(...args);
    const message = args.map(a => typeof a === 'object' ? JSON.stringify(a, null, 2) : String(a)).join(' ');

    // Categorize logs
    if (message.includes('AI Response') || message.includes('Parsed changes')) {
      addLog({ type: 'ai', category: 'AI', message });
    } else if (message.includes('API') || message.includes('fetch')) {
      addLog({ type: 'api', category: 'API', message });
    }
  };

  console.warn = (...args) => {
    originalConsole.warn(...args);
    const message = args.map(a => typeof a === 'object' ? JSON.stringify(a, null, 2) : String(a)).join(' ');
    addLog({ type: 'warning', category: 'Warning', message });
  };

  console.error = (...args) => {
    originalConsole.error(...args);
    const message = args.map(a => typeof a === 'object' ? JSON.stringify(a, null, 2) : String(a)).join(' ');
    addLog({ type: 'error', category: 'Error', message });
  };

  // Intercept window errors
  window.addEventListener('error', (event) => {
    addLog({
      type: 'error',
      category: 'Runtime Error',
      message: event.message,
      details: { filename: event.filename, lineno: event.lineno, colno: event.colno },
    });
  });

  window.addEventListener('unhandledrejection', (event) => {
    addLog({
      type: 'error',
      category: 'Unhandled Promise',
      message: String(event.reason),
    });
  });
};

export const DevConsole: React.FC<DevConsoleProps> = ({ className }) => {
  const [isExpanded, setIsExpanded] = useState(false);
  const [isMaximized, setIsMaximized] = useState(false);
  const [activeTab, setActiveTab] = useState('ai');
  const [copied, setCopied] = useState(false);
  const logs = useLogs();
  const scrollRef = useRef<HTMLDivElement>(null);

  // Auto-scroll to bottom when new logs arrive
  useEffect(() => {
    if (scrollRef.current && isExpanded) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [logs, isExpanded]);

  // Start intercepting console on mount
  useEffect(() => {
    interceptConsole();
  }, []);

  const filteredLogs = logs.filter(log => {
    if (activeTab === 'ai') return log.type === 'ai' || log.type === 'api';
    if (activeTab === 'audit') return log.type === 'change';
    if (activeTab === 'errors') return log.type === 'error' || log.type === 'warning';
    return true;
  });

  const errorCount = logs.filter(l => l.type === 'error').length;
  const warningCount = logs.filter(l => l.type === 'warning').length;

  const copyLogs = useCallback(() => {
    const text = filteredLogs
      .map(log => `[${log.timestamp.toISOString()}] [${log.category}] ${log.message}`)
      .join('\n');
    navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [filteredLogs]);

  const downloadLogs = useCallback(() => {
    const text = filteredLogs
      .map(log => `[${log.timestamp.toISOString()}] [${log.category}] ${log.message}${log.details ? '\n  Details: ' + JSON.stringify(log.details) : ''}`)
      .join('\n');
    const blob = new Blob([text], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `console-logs-${activeTab}-${new Date().toISOString().split('T')[0]}.txt`;
    a.click();
    URL.revokeObjectURL(url);
  }, [filteredLogs, activeTab]);

  const getLogColor = (type: LogEntry['type']) => {
    switch (type) {
      case 'error': return 'text-red-500';
      case 'warning': return 'text-yellow-500';
      case 'ai': return 'text-purple-500';
      case 'api': return 'text-blue-500';
      case 'change': return 'text-green-500';
      default: return 'text-gray-500';
    }
  };

  const getLogIcon = (type: LogEntry['type']) => {
    switch (type) {
      case 'error': return <AlertTriangle className="h-3 w-3" />;
      case 'warning': return <AlertTriangle className="h-3 w-3" />;
      case 'ai': return <Brain className="h-3 w-3" />;
      case 'change': return <FileText className="h-3 w-3" />;
      default: return <Terminal className="h-3 w-3" />;
    }
  };

  if (!isExpanded) {
    return (
      <div className={cn("fixed bottom-0 left-0 right-0 z-50", className)}>
        <div className="mx-4 mb-2">
          <Button
            onClick={() => setIsExpanded(true)}
            variant="outline"
            className="w-full h-8 bg-gray-900 border-gray-700 text-gray-300 hover:bg-gray-800 hover:text-white flex items-center justify-between px-4"
          >
            <div className="flex items-center gap-2">
              <Terminal className="h-4 w-4" />
              <span className="text-xs font-mono">Developer Console</span>
            </div>
            <div className="flex items-center gap-2">
              {errorCount > 0 && (
                <Badge variant="destructive" className="h-5 text-xs">
                  {errorCount} error{errorCount !== 1 ? 's' : ''}
                </Badge>
              )}
              {warningCount > 0 && (
                <Badge variant="outline" className="h-5 text-xs border-yellow-500 text-yellow-500">
                  {warningCount} warning{warningCount !== 1 ? 's' : ''}
                </Badge>
              )}
              <ChevronUp className="h-4 w-4" />
            </div>
          </Button>
        </div>
      </div>
    );
  }

  return (
    <div
      className={cn(
        "fixed bottom-0 left-0 right-0 z-50 transition-all duration-200",
        isMaximized ? "top-0" : "",
        className
      )}
    >
      <Card className={cn(
        "mx-2 mb-2 bg-gray-900 border-gray-700 text-gray-100 flex flex-col",
        isMaximized ? "h-full rounded-none mx-0 mb-0" : "h-80"
      )}>
        {/* Header */}
        <div className="flex items-center justify-between px-3 py-2 border-b border-gray-700 flex-shrink-0">
          <div className="flex items-center gap-2">
            <Terminal className="h-4 w-4 text-green-400" />
            <span className="text-sm font-mono font-semibold">Developer Console</span>
          </div>
          <div className="flex items-center gap-1">
            <Button
              variant="ghost"
              size="sm"
              onClick={copyLogs}
              className="h-7 w-7 p-0 text-gray-400 hover:text-white hover:bg-gray-700"
            >
              {copied ? <CheckCheck className="h-3.5 w-3.5 text-green-400" /> : <Copy className="h-3.5 w-3.5" />}
            </Button>
            <Button
              variant="ghost"
              size="sm"
              onClick={downloadLogs}
              className="h-7 w-7 p-0 text-gray-400 hover:text-white hover:bg-gray-700"
            >
              <Download className="h-3.5 w-3.5" />
            </Button>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => clearLogs(activeTab)}
              className="h-7 w-7 p-0 text-gray-400 hover:text-white hover:bg-gray-700"
            >
              <Trash2 className="h-3.5 w-3.5" />
            </Button>
            <div className="w-px h-4 bg-gray-700 mx-1" />
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setIsMaximized(!isMaximized)}
              className="h-7 w-7 p-0 text-gray-400 hover:text-white hover:bg-gray-700"
            >
              {isMaximized ? <Minimize2 className="h-3.5 w-3.5" /> : <Maximize2 className="h-3.5 w-3.5" />}
            </Button>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setIsExpanded(false)}
              className="h-7 w-7 p-0 text-gray-400 hover:text-white hover:bg-gray-700"
            >
              <ChevronDown className="h-3.5 w-3.5" />
            </Button>
          </div>
        </div>

        {/* Tabs */}
        <Tabs value={activeTab} onValueChange={setActiveTab} className="flex-1 flex flex-col min-h-0">
          <TabsList className="w-full justify-start rounded-none border-b border-gray-700 bg-transparent h-9 px-2">
            <TabsTrigger
              value="ai"
              className="text-xs data-[state=active]:bg-gray-700 data-[state=active]:text-purple-400 text-gray-400 rounded-sm px-3"
            >
              <Brain className="h-3 w-3 mr-1" />
              AI Thinking
            </TabsTrigger>
            <TabsTrigger
              value="audit"
              className="text-xs data-[state=active]:bg-gray-700 data-[state=active]:text-green-400 text-gray-400 rounded-sm px-3"
            >
              <FileText className="h-3 w-3 mr-1" />
              Audit Log
            </TabsTrigger>
            <TabsTrigger
              value="errors"
              className="text-xs data-[state=active]:bg-gray-700 data-[state=active]:text-red-400 text-gray-400 rounded-sm px-3"
            >
              <AlertTriangle className="h-3 w-3 mr-1" />
              Errors
              {(errorCount + warningCount) > 0 && (
                <Badge variant="destructive" className="ml-1 h-4 text-[10px] px-1">
                  {errorCount + warningCount}
                </Badge>
              )}
            </TabsTrigger>
          </TabsList>

          <div className="flex-1 min-h-0 overflow-hidden">
            <ScrollArea className="h-full" ref={scrollRef}>
              <div className="p-2 font-mono text-xs space-y-1">
                {filteredLogs.length === 0 ? (
                  <div className="text-gray-500 text-center py-8">
                    {activeTab === 'ai' && "AI activity logs will appear here..."}
                    {activeTab === 'audit' && "Change audit logs will appear here..."}
                    {activeTab === 'errors' && "No errors or warnings"}
                  </div>
                ) : (
                  filteredLogs.map((log) => (
                    <div
                      key={log.id}
                      className={cn(
                        "flex items-start gap-2 py-1 px-2 rounded hover:bg-gray-800/50",
                        log.type === 'error' && "bg-red-950/30"
                      )}
                    >
                      <span className="text-gray-500 flex-shrink-0">
                        {log.timestamp.toLocaleTimeString()}
                      </span>
                      <span className={cn("flex-shrink-0", getLogColor(log.type))}>
                        {getLogIcon(log.type)}
                      </span>
                      <span className={cn("flex-shrink-0 w-16", getLogColor(log.type))}>
                        [{log.category}]
                      </span>
                      <span className="text-gray-300 break-all whitespace-pre-wrap flex-1">
                        {log.message}
                      </span>
                    </div>
                  ))
                )}
              </div>
            </ScrollArea>
          </div>
        </Tabs>
      </Card>
    </div>
  );
};

export default DevConsole;

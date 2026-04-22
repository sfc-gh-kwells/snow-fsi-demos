"use client";

import { useState, useRef, useEffect, useCallback, useMemo } from "react";
import { Send, Bot, User, Sparkles, FileText, Scale, AlertTriangle, Building2, Loader2, Database, ChevronDown, ChevronUp, CheckCircle2, Copy, Check, BarChart3, Table2, Brain } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";
import ReactMarkdown from "react-markdown";
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend
} from "recharts";

interface ChartData {
  chart_spec: string;
}

interface Message {
  id: string;
  role: "user" | "assistant";
  content: string;
  status?: "thinking" | "sent" | "error";
  isStreaming?: boolean;
  streamingStatus?: string;
  thinkingSteps?: string[];
  thinkingContent?: string; // Full thinking/reasoning text
  sql?: string;
  error?: string;
  charts?: ChartData[];
}

interface ChatPanelProps {
  className?: string;
}

const SUGGESTIONS = [
  { icon: Scale, label: "Cross-default thresholds", query: "What are the current cross-default threshold amounts across all agreements?" },
  { icon: AlertTriangle, label: "Termination events", query: "List all termination events and how they trigger early termination" },
  { icon: Building2, label: "Barclays exposure", query: "What is the cross-default threshold for Barclays?" },
  { icon: FileText, label: "2002 vs 1992 ISDA", query: "Which agreements use 1992 vs 2002 ISDA Master Agreement?" },
];

const CHART_COLORS = ["#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#ec4899", "#06b6d4", "#84cc16"];

function getStatusLabel(status: string): string {
  const statusMap: Record<string, string> = {
    "planning": "Planning next steps",
    "extracting_tool_calls": "Selecting data sources",
    "reasoning_agent_stop": "Reviewing results",
    "proceeding_to_answer": "Forming answer",
    "tool_execution": "Executing query",
  };
  return statusMap[status] || status;
}

// Chart component that renders Vega-Lite spec using Recharts
function ChartVisualization({ chartSpec }: { chartSpec: string }) {
  const [viewMode, setViewMode] = useState<"chart" | "table">("chart");
  
  const { chartType, data, title, xKey, yKeys } = useMemo(() => {
    try {
      const spec = typeof chartSpec === "string" ? JSON.parse(chartSpec) : chartSpec;
      const rawData = spec.data?.values || [];
      const mark = typeof spec.mark === "string" ? spec.mark : spec.mark?.type || "bar";
      
      if (rawData.length === 0) {
        return { chartType: "bar", data: [], title: spec.title || "Chart", xKey: "", yKeys: [] };
      }
      
      // Auto-detect keys
      const keys = Object.keys(rawData[0] || {});
      const xKey = keys.find(k => 
        typeof rawData[0][k] === "string" || 
        k.toLowerCase().includes("name") ||
        k.toLowerCase().includes("month") ||
        k.toLowerCase().includes("date") ||
        k.toLowerCase().includes("category")
      ) || keys[0];
      
      const yKeys = keys.filter(k => 
        k !== xKey && 
        (typeof rawData[0][k] === "number" || !isNaN(Number(rawData[0][k])))
      );
      
      // Normalize data
      const normalizedData = rawData.map((item: Record<string, unknown>, idx: number) => ({
        ...item,
        name: item[xKey] || `Item ${idx + 1}`,
      }));
      
      return {
        chartType: mark,
        data: normalizedData,
        title: spec.title || "Chart",
        xKey,
        yKeys
      };
    } catch (e) {
      console.error("Chart parse error:", e);
      return { chartType: "bar", data: [], title: "Chart", xKey: "", yKeys: [] };
    }
  }, [chartSpec]);

  if (data.length === 0) {
    return (
      <div className="p-4 text-center text-slate-500 text-sm">
        No chart data available
      </div>
    );
  }

  return (
    <div className="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-2 border-b border-slate-200 dark:border-slate-700">
        <div className="flex items-center gap-2">
          <BarChart3 className="w-4 h-4 text-blue-500" />
          <span className="text-sm font-medium text-slate-700 dark:text-slate-300">{title}</span>
        </div>
        <div className="flex gap-1">
          <button
            onClick={() => setViewMode("chart")}
            className={cn(
              "p-1.5 rounded transition-colors",
              viewMode === "chart" 
                ? "bg-blue-100 dark:bg-blue-900/30 text-blue-600" 
                : "text-slate-400 hover:bg-slate-100 dark:hover:bg-slate-700"
            )}
          >
            <BarChart3 className="w-4 h-4" />
          </button>
          <button
            onClick={() => setViewMode("table")}
            className={cn(
              "p-1.5 rounded transition-colors",
              viewMode === "table" 
                ? "bg-blue-100 dark:bg-blue-900/30 text-blue-600" 
                : "text-slate-400 hover:bg-slate-100 dark:hover:bg-slate-700"
            )}
          >
            <Table2 className="w-4 h-4" />
          </button>
        </div>
      </div>
      
      {/* Content */}
      <div className="p-4">
        {viewMode === "chart" ? (
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              {chartType === "line" ? (
                <LineChart data={data}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                  <XAxis dataKey="name" tick={{ fontSize: 11 }} stroke="#94a3b8" />
                  <YAxis tick={{ fontSize: 11 }} stroke="#94a3b8" />
                  <Tooltip 
                    contentStyle={{ 
                      backgroundColor: "#fff", 
                      border: "1px solid #e2e8f0",
                      borderRadius: "8px",
                      fontSize: "12px"
                    }} 
                  />
                  <Legend />
                  {yKeys.map((key, idx) => (
                    <Line 
                      key={key} 
                      type="monotone" 
                      dataKey={key} 
                      stroke={CHART_COLORS[idx % CHART_COLORS.length]}
                      strokeWidth={2}
                      dot={{ r: 3 }}
                    />
                  ))}
                </LineChart>
              ) : chartType === "pie" ? (
                <PieChart>
                  <Pie
                    data={data}
                    dataKey={yKeys[0] || "value"}
                    nameKey="name"
                    cx="50%"
                    cy="50%"
                    outerRadius={80}
                    label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(0)}%`}
                  >
                    {data.map((_: unknown, idx: number) => (
                      <Cell key={idx} fill={CHART_COLORS[idx % CHART_COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip />
                </PieChart>
              ) : (
                <BarChart data={data}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                  <XAxis dataKey="name" tick={{ fontSize: 11 }} stroke="#94a3b8" />
                  <YAxis tick={{ fontSize: 11 }} stroke="#94a3b8" />
                  <Tooltip 
                    contentStyle={{ 
                      backgroundColor: "#fff", 
                      border: "1px solid #e2e8f0",
                      borderRadius: "8px",
                      fontSize: "12px"
                    }} 
                  />
                  <Legend />
                  {yKeys.map((key, idx) => (
                    <Bar 
                      key={key} 
                      dataKey={key} 
                      fill={CHART_COLORS[idx % CHART_COLORS.length]}
                      radius={[4, 4, 0, 0]}
                    />
                  ))}
                </BarChart>
              )}
            </ResponsiveContainer>
          </div>
        ) : (
          <div className="overflow-x-auto max-h-64">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-200 dark:border-slate-700">
                  <th className="text-left py-2 px-3 font-medium text-slate-600 dark:text-slate-400">{xKey}</th>
                  {yKeys.map(key => (
                    <th key={key} className="text-right py-2 px-3 font-medium text-slate-600 dark:text-slate-400">{key}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.map((row: Record<string, unknown>, idx: number) => (
                  <tr key={idx} className="border-b border-slate-100 dark:border-slate-800">
                    <td className="py-2 px-3 text-slate-700 dark:text-slate-300">{String(row.name)}</td>
                    {yKeys.map(key => (
                      <td key={key} className="text-right py-2 px-3 text-slate-700 dark:text-slate-300">
                        {typeof row[key] === "number" ? (row[key] as number).toLocaleString() : String(row[key])}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

export function ChatPanel({ className }: ChatPanelProps) {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const abortControllerRef = useRef<AbortController | null>(null);

  const [expandedThinking, setExpandedThinking] = useState<Record<string, boolean>>({});
  const [expandedSql, setExpandedSql] = useState<Record<string, boolean>>({});
  const [copiedSql, setCopiedSql] = useState<string | null>(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  const copyToClipboard = async (text: string, messageId: string) => {
    await navigator.clipboard.writeText(text);
    setCopiedSql(messageId);
    setTimeout(() => setCopiedSql(null), 2000);
  };

  const sendMessage = useCallback(async (messageText: string) => {
    if (!messageText.trim() || isLoading) return;

    const messageId = Date.now().toString();
    const userMessage: Message = { 
      id: messageId + "_user",
      role: "user", 
      content: messageText.trim() 
    };
    
    const assistantMessageId = messageId + "_assistant";
    const assistantMessage: Message = {
      id: assistantMessageId,
      role: "assistant",
      content: "",
      status: "thinking",
      isStreaming: true,
      streamingStatus: "Connecting...",
      thinkingSteps: [],
      thinkingContent: "",
      charts: []
    };

    const history = messages.filter(m => m.role === "user" || (m.role === "assistant" && m.content));

    setMessages(prev => [...prev, userMessage, assistantMessage]);
    setInput("");
    setIsLoading(true);

    const abortController = new AbortController();
    abortControllerRef.current = abortController;

    try {
      const response = await fetch("/api/chat", {
        method: "POST",
        headers: { 
          "Content-Type": "application/json",
          "Accept": "text/event-stream"
        },
        body: JSON.stringify({ 
          message: messageText.trim(),
          history: history.map(m => ({ role: m.role, content: m.content }))
        }),
        signal: abortController.signal
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`API error: ${response.status} - ${errorText}`);
      }

      const reader = response.body?.getReader();
      if (!reader) throw new Error("No response body");

      const decoder = new TextDecoder();
      let assistantText = "";
      let thinkingText = "";
      let assistantSql = "";
      let thinkingSteps: string[] = [];
      let charts: ChartData[] = [];
      let buffer = "";
      let isInThinkingPhase = true; // Track if we're in thinking phase

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        const chunk = decoder.decode(value, { stream: true });
        buffer += chunk;
        
        const events = buffer.split("\n\n");
        buffer = events.pop() || "";

        for (const eventBlock of events) {
          if (!eventBlock.trim()) continue;
          
          const lines = eventBlock.split("\n");
          let eventType = "";
          let dataStr = "";
          
          for (const line of lines) {
            if (line.startsWith("event:")) {
              eventType = line.slice(6).trim();
            } else if (line.startsWith("data:")) {
              dataStr = line.slice(5).trim();
            }
          }
          
          if (!dataStr || dataStr === "{}" || dataStr === "[DONE]") continue;

          try {
            const data = JSON.parse(dataStr);

            if (eventType === "thinking") {
              // Accumulate thinking content separately
              if (data.text) {
                thinkingText += data.text;
                setMessages(prev => prev.map(msg => 
                  msg.id === assistantMessageId 
                    ? { ...msg, thinkingContent: thinkingText, streamingStatus: "Analyzing..." }
                    : msg
                ));
              }
            }
            else if (eventType === "text" || eventType === "response.text.delta") {
              if (data.text) {
                // Once we start receiving text events, we're past thinking phase
                isInThinkingPhase = false;
                assistantText += data.text;
                setMessages(prev => prev.map(msg => 
                  msg.id === assistantMessageId 
                    ? { ...msg, content: assistantText, streamingStatus: undefined }
                    : msg
                ));
              }
            } 
            else if (eventType === "status" || eventType === "response.status") {
              const statusMsg = getStatusLabel(data.message || data.status || "Processing...");
              if (!thinkingSteps.includes(statusMsg)) {
                thinkingSteps = [...thinkingSteps, statusMsg];
                setMessages(prev => prev.map(msg => 
                  msg.id === assistantMessageId 
                    ? { ...msg, streamingStatus: statusMsg, thinkingSteps: thinkingSteps }
                    : msg
                ));
              }
            } 
            else if (eventType === "tool_result" || eventType === "response.tool_result") {
              if (data.sql) {
                assistantSql = data.sql;
                const sqlStep = "Query executed";
                if (!thinkingSteps.includes(sqlStep)) {
                  thinkingSteps = [...thinkingSteps, sqlStep];
                }
                setMessages(prev => prev.map(msg => 
                  msg.id === assistantMessageId 
                    ? { ...msg, sql: assistantSql, thinkingSteps: thinkingSteps }
                    : msg
                ));
              }
            }
            else if (eventType === "chart" || eventType === "response.chart") {
              if (data.chart_spec) {
                charts = [...charts, { chart_spec: data.chart_spec }];
                setMessages(prev => prev.map(msg => 
                  msg.id === assistantMessageId 
                    ? { ...msg, charts: charts }
                    : msg
                ));
              }
            }
            else if (eventType === "error") {
              const errorMsg = data.message || "An error occurred";
              setMessages(prev => prev.map(msg => 
                msg.id === assistantMessageId 
                  ? { ...msg, content: "", status: "error" as const, error: errorMsg, isStreaming: false }
                  : msg
              ));
            }
          } catch (e) {
            console.error("Parse error:", e);
          }
        }
      }

      setMessages(prev => prev.map(msg => 
        msg.id === assistantMessageId 
          ? { 
              ...msg, 
              content: assistantText || "I couldn't generate a response.", 
              status: "sent" as const,
              isStreaming: false,
              streamingStatus: undefined,
              sql: assistantSql || undefined,
              thinkingSteps: thinkingSteps,
              thinkingContent: thinkingText || undefined,
              charts: charts
            }
          : msg
      ));

    } catch (error) {
      if (error instanceof Error && error.name === "AbortError") {
        setMessages(prev => prev.map(msg => 
          msg.id === assistantMessageId 
            ? { ...msg, content: "", status: "error" as const, error: "Request cancelled", isStreaming: false }
            : msg
        ));
      } else {
        const errorMessage = error instanceof Error ? error.message : "Unknown error";
        setMessages(prev => prev.map(msg => 
          msg.id === assistantMessageId 
            ? { ...msg, content: "", status: "error" as const, error: errorMessage, isStreaming: false }
            : msg
        ));
      }
    } finally {
      setIsLoading(false);
      abortControllerRef.current = null;
      inputRef.current?.focus();
    }
  }, [isLoading, messages]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    sendMessage(input);
  };

  const toggleThinking = (messageId: string) => {
    setExpandedThinking(prev => ({ ...prev, [messageId]: !prev[messageId] }));
  };

  const toggleSql = (messageId: string) => {
    setExpandedSql(prev => ({ ...prev, [messageId]: !prev[messageId] }));
  };

  return (
    <div className={cn("flex flex-col h-full bg-slate-50 dark:bg-slate-900", className)}>
      {/* Header */}
      <div className="flex items-center gap-3 px-5 py-4 border-b bg-white dark:bg-slate-800 shadow-sm">
        <div className="flex items-center justify-center w-10 h-10 rounded-full bg-blue-600 text-white">
          <Sparkles className="w-5 h-5" />
        </div>
        <div>
          <h2 className="font-semibold text-slate-900 dark:text-white">ISDA Document Assistant</h2>
          <p className="text-xs text-slate-500 dark:text-slate-400">Powered by Snowflake Cortex</p>
        </div>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto" ref={scrollRef}>
        {messages.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full p-6">
            <div className="text-center space-y-4 max-w-md">
              <div className="flex items-center justify-center w-16 h-16 mx-auto rounded-full bg-blue-100 dark:bg-blue-900/30">
                <Bot className="w-8 h-8 text-blue-600 dark:text-blue-400" />
              </div>
              <div>
                <h3 className="text-lg font-semibold text-slate-900 dark:text-white mb-1">
                  How can I help you today?
                </h3>
                <p className="text-sm text-slate-500 dark:text-slate-400">
                  Ask questions about your ISDA Master Agreements, cross-default thresholds, termination events, and more.
                </p>
              </div>
            </div>
            
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 w-full max-w-lg mt-6">
              {SUGGESTIONS.map((suggestion, idx) => (
                <button
                  key={idx}
                  onClick={() => sendMessage(suggestion.query)}
                  className="flex items-center gap-3 p-3 text-left text-sm rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 hover:border-blue-400 dark:hover:border-blue-500 hover:shadow-sm transition-all group"
                >
                  <div className="flex items-center justify-center w-8 h-8 rounded-lg bg-slate-100 dark:bg-slate-700 group-hover:bg-blue-100 dark:group-hover:bg-blue-900/30 transition-colors">
                    <suggestion.icon className="w-4 h-4 text-slate-500 group-hover:text-blue-600 dark:group-hover:text-blue-400 transition-colors" />
                  </div>
                  <span className="text-slate-700 dark:text-slate-300 group-hover:text-slate-900 dark:group-hover:text-white font-medium">
                    {suggestion.label}
                  </span>
                </button>
              ))}
            </div>
          </div>
        ) : (
          <div className="p-4 space-y-6">
            {messages.map((message) => (
              <div key={message.id} className="space-y-2">
                {/* User Message */}
                {message.role === "user" && (
                  <div className="flex justify-end">
                    <div className="flex items-start gap-2 max-w-[85%]">
                      <div className="bg-blue-600 text-white rounded-2xl rounded-br-md px-4 py-2.5 shadow-sm">
                        <p className="text-sm">{message.content}</p>
                      </div>
                      <div className="flex items-center justify-center w-8 h-8 rounded-full bg-slate-200 dark:bg-slate-700 flex-shrink-0">
                        <User className="w-4 h-4 text-slate-600 dark:text-slate-300" />
                      </div>
                    </div>
                  </div>
                )}

                {/* Assistant Message */}
                {message.role === "assistant" && (
                  <div className="flex justify-start">
                    <div className="flex items-start gap-2 max-w-[85%]">
                      <div className="flex items-center justify-center w-8 h-8 rounded-full bg-blue-600 flex-shrink-0">
                        <Bot className="w-4 h-4 text-white" />
                      </div>
                      
                      <div className="space-y-2 flex-1 min-w-0">
                        {/* Thinking Section - Collapsible with full reasoning */}
                        {(message.thinkingContent || message.thinkingSteps?.length || message.isStreaming) && (
                          <div className="bg-amber-50 dark:bg-amber-900/20 rounded-xl border border-amber-200 dark:border-amber-800 overflow-hidden">
                            <button 
                              onClick={() => toggleThinking(message.id)}
                              className="flex items-center justify-between w-full px-3 py-2 hover:bg-amber-100/50 dark:hover:bg-amber-800/20 transition-colors"
                            >
                              <div className="flex items-center gap-2">
                                {message.isStreaming && !message.content ? (
                                  <Loader2 className="w-4 h-4 animate-spin text-amber-600" />
                                ) : (
                                  <Brain className="w-4 h-4 text-amber-600" />
                                )}
                                <span className="text-xs font-medium text-amber-700 dark:text-amber-300">
                                  {message.isStreaming && !message.content 
                                    ? (message.streamingStatus || "Thinking...") 
                                    : "Thinking & Planning"}
                                </span>
                                {message.thinkingSteps && message.thinkingSteps.length > 0 && (
                                  <span className="text-xs text-amber-600 dark:text-amber-400">
                                    ({message.thinkingSteps.length} steps)
                                  </span>
                                )}
                              </div>
                              {expandedThinking[message.id] ? (
                                <ChevronUp className="w-4 h-4 text-amber-500" />
                              ) : (
                                <ChevronDown className="w-4 h-4 text-amber-500" />
                              )}
                            </button>
                            
                            {expandedThinking[message.id] && (
                              <div className="px-3 pb-3 border-t border-amber-200 dark:border-amber-800">
                                {/* Status Steps */}
                                {message.thinkingSteps && message.thinkingSteps.length > 0 && (
                                  <div className="pt-2 space-y-1 mb-2">
                                    {message.thinkingSteps.map((step, idx) => (
                                      <div key={idx} className="flex items-center gap-2 text-xs">
                                        <CheckCircle2 className="w-3 h-3 text-green-500 flex-shrink-0" />
                                        <span className="text-amber-700 dark:text-amber-300">{step}</span>
                                      </div>
                                    ))}
                                    {message.isStreaming && message.streamingStatus && (
                                      <div className="flex items-center gap-2 text-xs">
                                        <Loader2 className="w-3 h-3 animate-spin text-amber-500 flex-shrink-0" />
                                        <span className="text-amber-600 dark:text-amber-400">{message.streamingStatus}</span>
                                      </div>
                                    )}
                                  </div>
                                )}
                                
                                {/* Full Thinking Content */}
                                {message.thinkingContent && (
                                  <div className="pt-2 border-t border-amber-200 dark:border-amber-700">
                                    <p className="text-xs text-amber-700 dark:text-amber-300 whitespace-pre-wrap leading-relaxed">
                                      {message.thinkingContent}
                                    </p>
                                  </div>
                                )}
                              </div>
                            )}
                          </div>
                        )}
                        
                        {/* SQL Query */}
                        {message.sql && (
                          <div className="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
                            <button 
                              onClick={() => toggleSql(message.id)}
                              className="flex items-center justify-between w-full px-3 py-2 hover:bg-slate-50 dark:hover:bg-slate-700/50 transition-colors"
                            >
                              <div className="flex items-center gap-2">
                                <Database className="w-4 h-4 text-emerald-500" />
                                <span className="text-xs font-medium text-slate-600 dark:text-slate-300">SQL Query</span>
                              </div>
                              <div className="flex items-center gap-1">
                                <button
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    copyToClipboard(message.sql!, message.id);
                                  }}
                                  className="p-1 hover:bg-slate-200 dark:hover:bg-slate-600 rounded transition-colors"
                                >
                                  {copiedSql === message.id ? (
                                    <Check className="w-3 h-3 text-green-500" />
                                  ) : (
                                    <Copy className="w-3 h-3 text-slate-400" />
                                  )}
                                </button>
                                {expandedSql[message.id] ? (
                                  <ChevronUp className="w-4 h-4 text-slate-400" />
                                ) : (
                                  <ChevronDown className="w-4 h-4 text-slate-400" />
                                )}
                              </div>
                            </button>
                            
                            {expandedSql[message.id] && (
                              <div className="border-t border-slate-100 dark:border-slate-700">
                                <pre className="p-3 text-xs font-mono text-slate-700 dark:text-slate-300 overflow-x-auto bg-slate-50 dark:bg-slate-900/50 whitespace-pre-wrap">
                                  {message.sql}
                                </pre>
                              </div>
                            )}
                          </div>
                        )}
                        
                        {/* Charts */}
                        {message.charts && message.charts.length > 0 && (
                          <div className="space-y-2">
                            {message.charts.map((chart, idx) => (
                              <ChartVisualization key={idx} chartSpec={chart.chart_spec} />
                            ))}
                          </div>
                        )}
                        
                        {/* Error State */}
                        {message.status === "error" && message.error && (
                          <div className="bg-red-50 dark:bg-red-900/20 rounded-xl border border-red-200 dark:border-red-800 px-4 py-3">
                            <div className="flex items-start gap-2">
                              <AlertTriangle className="w-4 h-4 text-red-500 flex-shrink-0 mt-0.5" />
                              <p className="text-sm text-red-700 dark:text-red-300">{message.error}</p>
                            </div>
                          </div>
                        )}
                        
                        {/* Main Response - Only final answer with Markdown rendering */}
                        {message.content && (
                          <div className="bg-white dark:bg-slate-800 rounded-2xl rounded-tl-md px-4 py-3 shadow-sm border border-slate-200 dark:border-slate-700">
                            <div className="text-sm text-slate-800 dark:text-slate-200 prose-chat">
                              <ReactMarkdown
                                components={{
                                  // Add proper spacing for paragraphs
                                  p: ({ children }) => <p className="mb-3 last:mb-0">{children}</p>,
                                  // Style headers
                                  h1: ({ children }) => <h1 className="text-lg font-semibold mt-4 mb-2 first:mt-0">{children}</h1>,
                                  h2: ({ children }) => <h2 className="text-base font-semibold mt-4 mb-2 first:mt-0">{children}</h2>,
                                  h3: ({ children }) => <h3 className="text-sm font-semibold mt-3 mb-2 first:mt-0">{children}</h3>,
                                  // Style lists with spacing
                                  ul: ({ children }) => <ul className="list-disc pl-5 my-3 space-y-1">{children}</ul>,
                                  ol: ({ children }) => <ol className="list-decimal pl-5 my-3 space-y-1">{children}</ol>,
                                  li: ({ children }) => <li className="mb-1">{children}</li>,
                                  // Style bold text
                                  strong: ({ children }) => <strong className="font-semibold">{children}</strong>,
                                }}
                              >
                                {message.content}
                              </ReactMarkdown>
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Input */}
      <div className="border-t bg-white dark:bg-slate-800 p-4">
        <form onSubmit={handleSubmit} className="flex gap-2">
          <Input
            ref={inputRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Ask a question about your ISDA documents..."
            disabled={isLoading}
            className="flex-1 bg-slate-50 dark:bg-slate-900 border-slate-200 dark:border-slate-700 focus:ring-blue-500 focus:border-blue-500"
          />
          <Button
            type="submit"
            disabled={isLoading || !input.trim()}
            className="bg-blue-600 hover:bg-blue-700 text-white px-4"
          >
            {isLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
          </Button>
        </form>
        <p className="text-xs text-slate-400 mt-2 text-center">
          Powered by Snowflake Cortex Agents
        </p>
      </div>
    </div>
  );
}

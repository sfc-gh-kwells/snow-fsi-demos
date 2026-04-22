import { NextRequest } from "next/server";
import { getAuthHeader, getHost } from "@/lib/snowflake";

const DATABASE = "ISDA_DOCUMENT_POC";
const SCHEMA = "SEMANTIC_VIEWS";
const AGENT_NAME = "ISDA_DOCUMENT_AGENT";

// Extend the timeout for this route (Cortex Agents can take a while)
export const maxDuration = 300; // 5 minutes
export const dynamic = "force-dynamic";

export async function POST(request: NextRequest) {
  try {
    const { message, history } = await request.json();
    
    const authHeader = getAuthHeader();
    const host = getHost();
    
    if (!authHeader) {
      return new Response(
        JSON.stringify({ error: "No authentication configured" }),
        { status: 401, headers: { "Content-Type": "application/json" } }
      );
    }
    
    const messages = [
      ...(history || []).map((msg: { role: string; content: string }) => ({
        role: msg.role,
        content: [{ type: "text", text: msg.content }]
      })),
      {
        role: "user",
        content: [{ type: "text", text: message }]
      }
    ];
    
    const agentEndpoint = `https://${host}/api/v2/databases/${DATABASE}/schemas/${SCHEMA}/agents/${AGENT_NAME}:run`;
    console.log("Calling agent endpoint:", agentEndpoint);
    console.log("Request messages:", JSON.stringify(messages).substring(0, 500));
    
    // Create AbortController with 5 minute timeout
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 300000); // 5 minutes
    
    try {
      const response = await fetch(agentEndpoint, {
        method: "POST",
        headers: {
          "Authorization": authHeader,
          "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
          "Content-Type": "application/json",
          "Accept": "text/event-stream"
        },
        body: JSON.stringify({ 
          messages,
          stream: true,
          tool_resources: {
            Analyst: {
              semantic_view: "ISDA_DOCUMENT_POC.SEMANTIC_VIEWS.ISDA_AGREEMENT_TERMS",
              execution_environment: {
                type: "warehouse",
                warehouse: "COMPUTE_WH"
              }
            },
            Search: {
              name: "ISDA_DOCUMENT_POC.SEMANTIC_VIEWS.ISDA_DOCUMENT_SEARCH",
              max_results: 5
            }
          }
        }),
        signal: controller.signal
      });
      
      clearTimeout(timeoutId);
      
      console.log("Response status:", response.status);
      console.log("Response content-type:", response.headers.get("content-type"));
      
      if (!response.ok) {
        const errorText = await response.text();
        console.error("Agent API error:", errorText);
        return new Response(
          JSON.stringify({ error: `Agent API error: ${response.status}`, details: errorText }),
          { status: response.status, headers: { "Content-Type": "application/json" } }
        );
      }
      
      // Check if response is SSE or JSON
      const contentType = response.headers.get("content-type") || "";
      
      // If it's not SSE, try to handle as JSON
      if (!contentType.includes("text/event-stream")) {
        console.log("Response is not SSE, content-type:", contentType);
        const text = await response.text();
        console.log("Raw response (first 1000 chars):", text.substring(0, 1000));
        
        // Try to parse as JSON and extract text
        try {
          const json = JSON.parse(text);
          console.log("Parsed JSON response");
          
          // Extract text from various possible formats
          let responseText = "";
          if (json.message?.content) {
            for (const content of json.message.content) {
              if (content.type === "text") {
                responseText += content.text || "";
              }
            }
          } else if (json.choices?.[0]?.message?.content) {
            responseText = json.choices[0].message.content;
          } else if (json.text) {
            responseText = json.text;
          }
          
          if (responseText) {
            const encoder = new TextEncoder();
            const stream = new ReadableStream({
              start(controller) {
                controller.enqueue(encoder.encode(`event: text\ndata: ${JSON.stringify({ text: responseText })}\n\n`));
                controller.enqueue(encoder.encode("event: done\ndata: {}\n\n"));
                controller.close();
              }
            });
            return new Response(stream, {
              headers: {
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive"
              }
            });
          }
        } catch (e) {
          console.error("Failed to parse JSON response:", e);
        }
      }
      
      // Stream the SSE response back to the client
      const encoder = new TextEncoder();
      const stream = new ReadableStream({
        async start(controller) {
          const reader = response.body?.getReader();
          if (!reader) {
            console.log("No reader available");
            controller.enqueue(encoder.encode(`event: error\ndata: ${JSON.stringify({ message: "No response body" })}\n\n`));
            controller.close();
            return;
          }
          
          const decoder = new TextDecoder();
          let buffer = "";
          let currentEvent = "";
          let eventCount = 0;
          
          try {
            while (true) {
              const { done, value } = await reader.read();
              if (done) {
                console.log("Stream done, total events:", eventCount);
                break;
              }
              
              const chunk = decoder.decode(value, { stream: true });
              console.log("Received chunk:", chunk.substring(0, 200));
              buffer += chunk;
              const lines = buffer.split("\n");
              buffer = lines.pop() || "";
              
              for (const line of lines) {
                // Log raw lines for debugging
                if (line.trim()) {
                  console.log("SSE line:", line.substring(0, 200));
                }
                
                // Parse event type
                if (line.startsWith("event: ")) {
                  currentEvent = line.slice(7).trim();
                  console.log("Event type:", currentEvent);
                  continue;
                }
                
                // Parse data
                if (line.startsWith("data: ")) {
                  const dataStr = line.slice(6).trim();
                  if (!dataStr || dataStr === "[DONE]") {
                    if (dataStr === "[DONE]") {
                      controller.enqueue(encoder.encode("event: done\ndata: {}\n\n"));
                    }
                    continue;
                  }
                  
                  try {
                    const parsed = JSON.parse(dataStr);
                    eventCount++;
                    console.log("Parsed event #", eventCount, "type:", currentEvent, "data:", JSON.stringify(parsed).substring(0, 200));
                    
                    // Handle different v2 event types
                    switch (currentEvent) {
                      case "error":
                        // Forward error to client
                        controller.enqueue(encoder.encode(
                          `event: error\ndata: ${JSON.stringify({ message: parsed.message || "Unknown error" })}\n\n`
                        ));
                        break;
                        
                      case "response.status":
                        // Forward status updates
                        controller.enqueue(encoder.encode(
                          `event: status\ndata: ${JSON.stringify({ 
                            status: parsed.status, 
                            message: parsed.message 
                          })}\n\n`
                        ));
                        break;
                        
                      case "response.text.delta":
                        // Stream text deltas
                        if (parsed.text) {
                          controller.enqueue(encoder.encode(
                            `event: text\ndata: ${JSON.stringify({ text: parsed.text })}\n\n`
                          ));
                        }
                        break;
                        
                      case "response.text":
                        // Complete text response - SKIP this as it duplicates the delta events
                        // The text was already streamed via response.text.delta
                        console.log("Skipping response.text (already streamed via deltas)");
                        break;
                        
                      case "response.thinking.delta":
                        // Forward thinking updates
                        if (parsed.text) {
                          controller.enqueue(encoder.encode(
                            `event: thinking\ndata: ${JSON.stringify({ text: parsed.text })}\n\n`
                          ));
                        }
                        break;
                        
                      case "response.tool_result":
                        // Handle tool results (SQL queries, etc.)
                        const toolData: { sql?: string; text?: string } = {};
                        if (parsed.tool_results?.content?.json) {
                          const json = parsed.tool_results.content.json;
                          if (json.sql) toolData.sql = json.sql;
                          if (json.text) toolData.text = json.text;
                        }
                        if (toolData.sql || toolData.text) {
                          controller.enqueue(encoder.encode(
                            `event: tool_result\ndata: ${JSON.stringify(toolData)}\n\n`
                          ));
                        }
                        break;
                        
                      case "message.delta":
                        // Handle legacy message delta format
                        const delta = parsed.delta;
                        if (delta?.content) {
                          for (const content of Array.isArray(delta.content) ? delta.content : [delta.content]) {
                            if (content.type === "text" && content.text) {
                              controller.enqueue(encoder.encode(
                                `event: text\ndata: ${JSON.stringify({ text: content.text })}\n\n`
                              ));
                            } else if (content.type === "tool_results") {
                              const json = content.tool_results?.content?.json;
                              if (json?.text) {
                                controller.enqueue(encoder.encode(
                                  `event: text\ndata: ${JSON.stringify({ text: json.text })}\n\n`
                                ));
                              }
                              if (json?.sql) {
                                controller.enqueue(encoder.encode(
                                  `event: tool_result\ndata: ${JSON.stringify({ sql: json.sql })}\n\n`
                                ));
                              }
                            }
                          }
                        }
                        break;
                        
                      case "response":
                        // Final response event - DO NOT extract text here as it duplicates stream content
                        // Just log for debugging
                        console.log("Final response event received");
                        break;
                      
                      case "response.chart":
                        // Handle chart data (Vega-Lite spec)
                        if (parsed.chart_spec) {
                          controller.enqueue(encoder.encode(
                            `event: chart\ndata: ${JSON.stringify({ chart_spec: parsed.chart_spec })}\n\n`
                          ));
                        }
                        break;
                        
                      default:
                        // Log unknown events for debugging but don't forward to avoid duplication
                        console.log("Unknown event type:", currentEvent, "data:", JSON.stringify(parsed).substring(0, 100));
                    }
                  } catch (e) {
                    console.error("Parse error:", e);
                  }
                }
              }
            }
          } catch (streamError) {
            console.error("Stream error:", streamError);
            controller.enqueue(encoder.encode(
              `event: error\ndata: ${JSON.stringify({ message: "Stream error" })}\n\n`
            ));
          } finally {
            controller.enqueue(encoder.encode("event: done\ndata: {}\n\n"));
            controller.close();
          }
        }
      });
      
      return new Response(stream, {
        headers: {
          "Content-Type": "text/event-stream",
          "Cache-Control": "no-cache, no-transform",
          "Connection": "keep-alive",
          "X-Accel-Buffering": "no"
        }
      });
    } catch (fetchError) {
      clearTimeout(timeoutId);
      throw fetchError;
    }
  } catch (error) {
    console.error("Chat error:", error);
    return new Response(
      JSON.stringify({ error: "Failed to process chat request", details: String(error) }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
}

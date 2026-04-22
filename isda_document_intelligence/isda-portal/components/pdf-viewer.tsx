"use client";

import { useState, useEffect } from "react";
import { FileText, ZoomIn, ZoomOut, Download, Loader2, AlertCircle } from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

interface PDFViewerProps {
  documentId: string | null;
  fileName?: string;
  className?: string;
}

export function PDFViewer({ documentId, fileName, className }: PDFViewerProps) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [scale, setScale] = useState(1);

  // The PDF URL uses our proxy endpoint
  const pdfUrl = documentId ? `/api/pdf/${documentId}?proxy=true` : null;
  const downloadUrl = documentId ? `/api/pdf/${documentId}` : null;

  useEffect(() => {
    if (!documentId) {
      return;
    }

    // Check if PDF is available
    const checkPdf = async () => {
      setLoading(true);
      setError(null);
      try {
        const response = await fetch(`/api/pdf/${documentId}`);
        if (!response.ok) {
          const errData = await response.json();
          throw new Error(errData.error || "Failed to load PDF");
        }
        // PDF is available
      } catch (err) {
        console.error("PDF check error:", err);
        setError(err instanceof Error ? err.message : "Unable to load PDF");
      } finally {
        setLoading(false);
      }
    };

    checkPdf();
  }, [documentId]);

  const handleZoomIn = () => setScale((s) => Math.min(s + 0.25, 3));
  const handleZoomOut = () => setScale((s) => Math.max(s - 0.25, 0.5));

  if (!documentId) {
    return (
      <div className={cn("flex flex-col items-center justify-center h-full bg-slate-50 dark:bg-slate-900 rounded-xl", className)}>
        <div className="flex items-center justify-center w-20 h-20 rounded-2xl bg-slate-100 dark:bg-slate-800 mb-4">
          <FileText className="w-10 h-10 text-slate-400" />
        </div>
        <p className="text-slate-500 text-sm">Select a document to view</p>
      </div>
    );
  }

  if (loading) {
    return (
      <div className={cn("flex flex-col items-center justify-center h-full bg-slate-50 dark:bg-slate-900 rounded-xl", className)}>
        <Loader2 className="w-8 h-8 text-blue-500 animate-spin mb-4" />
        <p className="text-slate-500 text-sm">Loading document...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className={cn("flex flex-col items-center justify-center h-full bg-slate-50 dark:bg-slate-900 rounded-xl", className)}>
        <div className="flex items-center justify-center w-16 h-16 rounded-full bg-red-100 dark:bg-red-900/30 mb-4">
          <AlertCircle className="w-8 h-8 text-red-500" />
        </div>
        <p className="text-slate-600 dark:text-slate-400 text-sm text-center max-w-xs">{error}</p>
      </div>
    );
  }

  return (
    <div className={cn("flex flex-col h-full bg-slate-100 dark:bg-slate-900 rounded-xl overflow-hidden", className)}>
      {/* Toolbar */}
      <div className="flex items-center justify-between px-4 py-2 bg-white dark:bg-slate-800 border-b border-slate-200 dark:border-slate-700">
        <div className="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-400">
          <FileText className="w-4 h-4" />
          <span className="truncate max-w-[200px]">{fileName || "Document"}</span>
        </div>
        <div className="flex items-center gap-1">
          <Button variant="ghost" size="sm" onClick={handleZoomOut} className="h-8 w-8 p-0">
            <ZoomOut className="w-4 h-4" />
          </Button>
          <span className="text-xs text-slate-500 w-12 text-center">{Math.round(scale * 100)}%</span>
          <Button variant="ghost" size="sm" onClick={handleZoomIn} className="h-8 w-8 p-0">
            <ZoomIn className="w-4 h-4" />
          </Button>
          {downloadUrl && (
            <Button variant="ghost" size="sm" asChild className="h-8 w-8 p-0 ml-2">
              <a href={`${downloadUrl}?proxy=true`} download={fileName}>
                <Download className="w-4 h-4" />
              </a>
            </Button>
          )}
        </div>
      </div>

      {/* PDF Display */}
      <div className="flex-1 overflow-auto p-4">
        {pdfUrl ? (
          <div 
            className="flex justify-center"
            style={{ transform: `scale(${scale})`, transformOrigin: 'top center' }}
          >
            <iframe
              src={pdfUrl}
              className="w-full h-[800px] bg-white rounded-lg shadow-lg"
              title="PDF Document"
            />
          </div>
        ) : (
          <div className="flex items-center justify-center h-full">
            <p className="text-slate-500">PDF preview not available</p>
          </div>
        )}
      </div>
    </div>
  );
}

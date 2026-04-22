"use client";

import { useState, useRef } from "react";
import { Upload, FileUp, CheckCircle2, Loader2, AlertCircle, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { cn } from "@/lib/utils";

interface UploadDialogProps {
  onUploadComplete?: () => void;
}

type UploadStatus = "idle" | "uploading" | "processing" | "success" | "error";

export function UploadDialog({ onUploadComplete }: UploadDialogProps) {
  const [open, setOpen] = useState(false);
  const [file, setFile] = useState<File | null>(null);
  const [documentType, setDocumentType] = useState<string>("");
  const [status, setStatus] = useState<UploadStatus>("idle");
  const [error, setError] = useState<string | null>(null);
  const [dragActive, setDragActive] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  
  // Upload is enabled - Snowflake stage is configured
  const uploadEnabled = true;

  const handleDrag = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);

    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      const droppedFile = e.dataTransfer.files[0];
      if (droppedFile.type === "application/pdf") {
        setFile(droppedFile);
        setError(null);
      } else {
        setError("Please upload a PDF file");
      }
    }
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      const selectedFile = e.target.files[0];
      if (selectedFile.type === "application/pdf") {
        setFile(selectedFile);
        setError(null);
      } else {
        setError("Please upload a PDF file");
      }
    }
  };

  const handleUpload = async () => {
    if (!file || !documentType) return;

    setStatus("uploading");
    setError(null);

    try {
      const formData = new FormData();
      formData.append("file", file);
      formData.append("documentType", documentType);

      setStatus("processing");

      const response = await fetch("/api/upload", {
        method: "POST",
        body: formData,
      });

      const result = await response.json();

      if (!response.ok) {
        throw new Error(result.details || result.error || "Upload failed");
      }

      if (result.warning) {
        console.warn("Upload warning:", result.warning);
      }

      setStatus("success");
      setTimeout(() => {
        setOpen(false);
        setFile(null);
        setDocumentType("");
        setStatus("idle");
        onUploadComplete?.();
      }, 1500);
    } catch (err) {
      console.error("Upload error:", err);
      setStatus("error");
      setError("Failed to upload document. Please try again.");
    }
  };

  const resetState = () => {
    setFile(null);
    setDocumentType("");
    setStatus("idle");
    setError(null);
  };

  return (
    <Dialog open={open} onOpenChange={(isOpen) => {
      setOpen(isOpen);
      if (!isOpen) resetState();
    }}>
      <DialogTrigger asChild>
        <Button className="bg-gradient-to-r from-blue-500 to-indigo-600 hover:from-blue-600 hover:to-indigo-700 text-white shadow-md">
          <Upload className="w-4 h-4 mr-2" />
          Upload Document
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Upload ISDA Document</DialogTitle>
          <DialogDescription>
            Upload a new contract to process with AI extraction
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-4">
          {/* Status Display */}
          {status === "success" && (
            <div className="flex items-center gap-3 p-4 rounded-xl bg-emerald-50 dark:bg-emerald-950 border border-emerald-200 dark:border-emerald-800">
              <CheckCircle2 className="w-6 h-6 text-emerald-500" />
              <div>
                <p className="font-medium text-emerald-700 dark:text-emerald-300">Upload Complete!</p>
                <p className="text-sm text-emerald-600 dark:text-emerald-400">Document processed successfully</p>
              </div>
            </div>
          )}

          {status === "error" && error && (
            <div className="flex items-center gap-3 p-4 rounded-xl bg-red-50 dark:bg-red-950 border border-red-200 dark:border-red-800">
              <AlertCircle className="w-6 h-6 text-red-500" />
              <div>
                <p className="font-medium text-red-700 dark:text-red-300">Upload Failed</p>
                <p className="text-sm text-red-600 dark:text-red-400">{error}</p>
              </div>
            </div>
          )}

          {(status === "uploading" || status === "processing") && (
            <div className="flex items-center gap-3 p-4 rounded-xl bg-blue-50 dark:bg-blue-950 border border-blue-200 dark:border-blue-800">
              <Loader2 className="w-6 h-6 text-blue-500 animate-spin" />
              <div>
                <p className="font-medium text-blue-700 dark:text-blue-300">
                  {status === "uploading" ? "Uploading..." : "Processing with AI..."}
                </p>
                <p className="text-sm text-blue-600 dark:text-blue-400">
                  {status === "uploading" 
                    ? "Sending document to Snowflake" 
                    : "Running PARSE_DOCUMENT and AI_EXTRACT"}
                </p>
              </div>
            </div>
          )}

          {status === "idle" && !uploadEnabled && (
            <div className="flex flex-col items-center justify-center p-8 text-center">
              <div className="flex items-center justify-center w-12 h-12 rounded-xl bg-amber-100 dark:bg-amber-900 mb-3">
                <AlertCircle className="w-6 h-6 text-amber-500" />
              </div>
              <p className="font-medium text-slate-900 dark:text-white mb-2">
                Configuration Required
              </p>
              <p className="text-sm text-slate-500 dark:text-slate-400 mb-4">
                Document upload requires a Snowflake stage to be configured. Please contact your administrator to set up:
              </p>
              <div className="text-xs text-left bg-slate-100 dark:bg-slate-800 rounded-lg p-3 font-mono w-full">
                <p>ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.ISDA_DOCUMENTS (stage)</p>
                <p>ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.RAW_DOCUMENT_METADATA (table)</p>
              </div>
            </div>
          )}

          {status === "idle" && uploadEnabled && (
            <>
              {/* Drop Zone */}
              <div
                onDragEnter={handleDrag}
                onDragLeave={handleDrag}
                onDragOver={handleDrag}
                onDrop={handleDrop}
                onClick={() => inputRef.current?.click()}
                className={cn(
                  "relative flex flex-col items-center justify-center p-8 border-2 border-dashed rounded-xl cursor-pointer transition-all",
                  dragActive
                    ? "border-blue-500 bg-blue-50 dark:bg-blue-950"
                    : file
                    ? "border-emerald-300 bg-emerald-50 dark:bg-emerald-950"
                    : "border-slate-300 dark:border-slate-700 hover:border-blue-400 hover:bg-slate-50 dark:hover:bg-slate-900"
                )}
              >
                <input
                  ref={inputRef}
                  type="file"
                  accept=".pdf"
                  onChange={handleFileChange}
                  className="hidden"
                />
                
                {file ? (
                  <>
                    <div className="flex items-center justify-center w-12 h-12 rounded-xl bg-emerald-100 dark:bg-emerald-900 mb-3">
                      <CheckCircle2 className="w-6 h-6 text-emerald-500" />
                    </div>
                    <p className="font-medium text-slate-900 dark:text-white">{file.name}</p>
                    <p className="text-sm text-slate-500 mt-1">
                      {(file.size / 1024 / 1024).toFixed(2)} MB
                    </p>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="mt-2"
                      onClick={(e) => {
                        e.stopPropagation();
                        setFile(null);
                      }}
                    >
                      <X className="w-4 h-4 mr-1" />
                      Remove
                    </Button>
                  </>
                ) : (
                  <>
                    <div className="flex items-center justify-center w-12 h-12 rounded-xl bg-slate-100 dark:bg-slate-800 mb-3">
                      <FileUp className="w-6 h-6 text-slate-400" />
                    </div>
                    <p className="font-medium text-slate-900 dark:text-white">
                      Drop your PDF here
                    </p>
                    <p className="text-sm text-slate-500 mt-1">
                      or click to browse
                    </p>
                  </>
                )}
              </div>

              {/* Document Type */}
              <div className="space-y-2">
                <label className="text-sm font-medium text-slate-700 dark:text-slate-300">
                  Document Type
                </label>
                <Select value={documentType} onValueChange={setDocumentType}>
                  <SelectTrigger>
                    <SelectValue placeholder="Select document type" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="MASTER_AGREEMENT">ISDA Master Agreement</SelectItem>
                    <SelectItem value="AMENDMENT">Amendment</SelectItem>
                    <SelectItem value="CSA">Credit Support Annex (CSA)</SelectItem>
                    <SelectItem value="SCHEDULE">Schedule</SelectItem>
                    <SelectItem value="CONFIRMATION">Trade Confirmation</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </>
          )}
        </div>

        {status === "idle" && uploadEnabled && (
          <div className="flex justify-end gap-2">
            <Button variant="outline" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button
              onClick={handleUpload}
              disabled={!file || !documentType}
              className="bg-gradient-to-r from-blue-500 to-indigo-600 hover:from-blue-600 hover:to-indigo-700 text-white"
            >
              <Upload className="w-4 h-4 mr-2" />
              Upload & Process
            </Button>
          </div>
        )}

        {status === "idle" && !uploadEnabled && (
          <div className="flex justify-end">
            <Button variant="outline" onClick={() => setOpen(false)}>
              Close
            </Button>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}

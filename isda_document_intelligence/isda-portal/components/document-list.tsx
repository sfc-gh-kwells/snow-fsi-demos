"use client";

import { useState, useEffect } from "react";
import { 
  FileText, Building2, Calendar, Shield, AlertTriangle, 
  CheckCircle2, Clock, Search, Filter, ChevronRight
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { cn } from "@/lib/utils";
import { format } from "date-fns";

interface Document {
  DOCUMENT_ID: string;
  FILE_NAME: string;
  DOCUMENT_TYPE: string;
  UPLOAD_TIMESTAMP: string;
  PROCESSING_STATUS: string;
  PAGE_COUNT: number;
  PARTY_A_NAME: string;
  PARTY_B_NAME: string;
  AGREEMENT_VERSION: string;
  EFFECTIVE_DATE: string;
  GOVERNING_LAW: string;
  CROSS_DEFAULT_APPLICABLE: boolean;
  CROSS_DEFAULT_THRESHOLD_AMOUNT: number;
  AUTOMATIC_EARLY_TERMINATION_PARTY_A: boolean;
  AUTOMATIC_EARLY_TERMINATION_PARTY_B: boolean;
}

interface DocumentListProps {
  selectedId: string | null;
  onSelect: (doc: Document) => void;
  className?: string;
}

export function DocumentList({ selectedId, onSelect, className }: DocumentListProps) {
  const [documents, setDocuments] = useState<Document[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [typeFilter, setTypeFilter] = useState<string>("all");

  useEffect(() => {
    const fetchDocuments = async () => {
      try {
        const response = await fetch("/api/documents");
        if (response.ok) {
          const data = await response.json();
          setDocuments(data);
        }
      } catch (error) {
        console.error("Failed to fetch documents:", error);
      } finally {
        setLoading(false);
      }
    };
    fetchDocuments();
  }, []);

  const filteredDocs = documents.filter((doc) => {
    // Only show PDF files
    const isPdf = doc.FILE_NAME?.toLowerCase().endsWith('.pdf');
    if (!isPdf) return false;
    
    const matchesSearch = 
      doc.FILE_NAME?.toLowerCase().includes(search.toLowerCase()) ||
      doc.PARTY_A_NAME?.toLowerCase().includes(search.toLowerCase()) ||
      doc.PARTY_B_NAME?.toLowerCase().includes(search.toLowerCase());
    
    const matchesType = typeFilter === "all" || doc.DOCUMENT_TYPE === typeFilter;
    
    return matchesSearch && matchesType;
  });

  const pdfDocuments = documents.filter(d => d.FILE_NAME?.toLowerCase().endsWith('.pdf'));
  const docTypes = [...new Set(pdfDocuments.map((d) => d.DOCUMENT_TYPE).filter(Boolean))];

  const getDocTypeIcon = (type: string) => {
    switch (type) {
      case "MASTER_AGREEMENT":
        return <FileText className="w-4 h-4" />;
      case "AMENDMENT":
        return <AlertTriangle className="w-4 h-4" />;
      case "CSA":
        return <Shield className="w-4 h-4" />;
      default:
        return <FileText className="w-4 h-4" />;
    }
  };

  const getDocTypeBadge = (type: string) => {
    switch (type) {
      case "MASTER_AGREEMENT":
        return <Badge variant="default" className="bg-blue-500/10 text-blue-600 border-blue-200">Master</Badge>;
      case "AMENDMENT":
        return <Badge variant="default" className="bg-amber-500/10 text-amber-600 border-amber-200">Amendment</Badge>;
      case "CSA":
        return <Badge variant="default" className="bg-emerald-500/10 text-emerald-600 border-emerald-200">CSA</Badge>;
      default:
        return <Badge variant="secondary">{type}</Badge>;
    }
  };

  if (loading) {
    return (
      <div className={cn("flex flex-col h-full", className)}>
        <div className="p-4 border-b">
          <Skeleton className="h-10 w-full" />
        </div>
        <div className="flex-1 p-4 space-y-3">
          {[1, 2, 3, 4, 5].map((i) => (
            <Skeleton key={i} className="h-24 w-full" />
          ))}
        </div>
      </div>
    );
  }

  return (
    <div className={cn("flex flex-col h-full bg-white dark:bg-slate-950", className)}>
      {/* Search & Filter Header */}
      <div className="p-4 space-y-3 border-b bg-slate-50 dark:bg-slate-900">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
          <Input
            placeholder="Search documents or parties..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="pl-9 bg-white dark:bg-slate-800"
          />
        </div>
        <Select value={typeFilter} onValueChange={setTypeFilter}>
          <SelectTrigger className="bg-white dark:bg-slate-800">
            <Filter className="w-4 h-4 mr-2 text-slate-400" />
            <SelectValue placeholder="Filter by type" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Documents</SelectItem>
            {docTypes.map((type) => (
              <SelectItem key={type} value={type}>
                {type.replace(/_/g, " ")}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {/* Stats Bar */}
      <div className="flex items-center justify-between px-4 py-2 text-xs text-slate-500 border-b">
        <span>{filteredDocs.length} documents</span>
        <span>{pdfDocuments.filter(d => d.DOCUMENT_TYPE === "MASTER_AGREEMENT" || d.DOCUMENT_TYPE?.includes("MASTER")).length} master agreements</span>
      </div>

      {/* Document List */}
      <div className="flex-1 min-h-0 overflow-y-auto">
        <div className="p-2 space-y-2">
          {filteredDocs.map((doc) => (
            <button
              key={doc.DOCUMENT_ID}
              onClick={() => onSelect(doc)}
              className={cn(
                "w-full text-left p-4 rounded-xl border transition-all",
                "hover:bg-slate-50 dark:hover:bg-slate-800 hover:border-blue-200 dark:hover:border-blue-800",
                selectedId === doc.DOCUMENT_ID
                  ? "bg-blue-50 dark:bg-blue-950 border-blue-300 dark:border-blue-700 shadow-sm"
                  : "bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800"
              )}
            >
              <div className="flex items-start justify-between gap-2 mb-2">
                <div className="flex items-center gap-2">
                  <div className={cn(
                    "p-2 rounded-lg",
                    selectedId === doc.DOCUMENT_ID
                      ? "bg-blue-100 dark:bg-blue-900"
                      : "bg-slate-100 dark:bg-slate-800"
                  )}>
                    {getDocTypeIcon(doc.DOCUMENT_TYPE)}
                  </div>
                  <div>
                    <p className="font-medium text-sm text-slate-900 dark:text-white line-clamp-1">
                      {doc.FILE_NAME}
                    </p>
                    {doc.AGREEMENT_VERSION && (
                      <p className="text-xs text-slate-500">
                        ISDA {doc.AGREEMENT_VERSION}
                      </p>
                    )}
                  </div>
                </div>
                {getDocTypeBadge(doc.DOCUMENT_TYPE)}
              </div>

              {(doc.PARTY_A_NAME || doc.PARTY_B_NAME) && (
                <div className="flex items-center gap-2 mt-3 text-xs text-slate-600 dark:text-slate-400">
                  <Building2 className="w-3 h-3" />
                  <span className="truncate">
                    {doc.PARTY_A_NAME} {doc.PARTY_B_NAME && `↔ ${doc.PARTY_B_NAME}`}
                  </span>
                </div>
              )}

              <div className="flex items-center gap-4 mt-2 text-xs text-slate-500">
                {doc.EFFECTIVE_DATE && (
                  <span className="flex items-center gap-1">
                    <Calendar className="w-3 h-3" />
                    {format(new Date(doc.EFFECTIVE_DATE), "MMM d, yyyy")}
                  </span>
                )}
                {doc.PAGE_COUNT && (
                  <span>{doc.PAGE_COUNT} pages</span>
                )}
              </div>

              {doc.CROSS_DEFAULT_APPLICABLE && (
                <div className="flex items-center gap-2 mt-2">
                  <Badge variant="outline" className="text-xs bg-slate-50 dark:bg-slate-800">
                    <Shield className="w-3 h-3 mr-1" />
                    Cross-default
                  </Badge>
                  {(doc.AUTOMATIC_EARLY_TERMINATION_PARTY_A || doc.AUTOMATIC_EARLY_TERMINATION_PARTY_B) && (
                    <Badge variant="outline" className="text-xs bg-slate-50 dark:bg-slate-800">
                      <AlertTriangle className="w-3 h-3 mr-1" />
                      AET
                    </Badge>
                  )}
                </div>
              )}
            </button>
          ))}

          {filteredDocs.length === 0 && (
            <div className="flex flex-col items-center justify-center py-12 text-slate-500">
              <FileText className="w-12 h-12 mb-4 text-slate-300" />
              <p className="text-sm">No documents found</p>
              <p className="text-xs">Try adjusting your search or filters</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

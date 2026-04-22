"use client";

import { useState, useEffect } from "react";
import { 
  CheckCircle2, XCircle, AlertCircle, Edit2, Save, 
  FileCheck, Building2, Calendar, Scale, Shield, 
  AlertTriangle, Loader2, CheckCircle, ClipboardCheck
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Separator } from "@/components/ui/separator";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog";
import { cn } from "@/lib/utils";

interface DocumentDetails {
  DOCUMENT_ID: string;
  FILE_NAME: string;
  PARTY_A_NAME: string;
  PARTY_B_NAME: string;
  AGREEMENT_VERSION: string;
  EFFECTIVE_DATE: string;
  GOVERNING_LAW: string;
  EVENTS_OF_DEFAULT: string;
  TERMINATION_EVENTS: string;
  CROSS_DEFAULT_APPLICABLE: boolean;
  CROSS_DEFAULT_THRESHOLD_AMOUNT: number;
  CROSS_DEFAULT_THRESHOLD_CURRENCY: string;
  AUTOMATIC_EARLY_TERMINATION_PARTY_A: boolean;
  AUTOMATIC_EARLY_TERMINATION_PARTY_B: boolean;
  CLOSE_OUT_CALCULATION: string;
  SET_OFF_RIGHTS: boolean;
}

interface VerificationStatus {
  VERIFICATION_ID: string;
  VERIFIED_BY: string;
  VERIFIED_AT: string;
  VERIFICATION_STATUS: string;
}

interface FieldStatus {
  verified: boolean;
  edited: boolean;
  editedValue?: string;
}

interface VerificationScreenProps {
  documentId: string | null;
  fileName?: string;
  onVerificationComplete?: () => void;
  className?: string;
}

const FIELD_LABELS: Record<string, string> = {
  PARTY_A_NAME: "Party A",
  PARTY_B_NAME: "Party B",
  AGREEMENT_VERSION: "ISDA Version",
  EFFECTIVE_DATE: "Effective Date",
  GOVERNING_LAW: "Governing Law",
  CROSS_DEFAULT_APPLICABLE: "Cross-Default Applicable",
  CROSS_DEFAULT_THRESHOLD_AMOUNT: "Cross-Default Threshold",
  AUTOMATIC_EARLY_TERMINATION_PARTY_A: "AET Party A",
  AUTOMATIC_EARLY_TERMINATION_PARTY_B: "AET Party B",
  CLOSE_OUT_CALCULATION: "Close-out Method",
  SET_OFF_RIGHTS: "Set-off Rights",
};

export function VerificationScreen({ 
  documentId, 
  fileName,
  onVerificationComplete,
  className 
}: VerificationScreenProps) {
  const [details, setDetails] = useState<DocumentDetails | null>(null);
  const [loading, setLoading] = useState(false);
  const [verificationStatus, setVerificationStatus] = useState<VerificationStatus | null>(null);
  const [fieldStatuses, setFieldStatuses] = useState<Record<string, FieldStatus>>({});
  const [notes, setNotes] = useState("");
  const [verifying, setVerifying] = useState(false);
  const [showConfirmDialog, setShowConfirmDialog] = useState(false);

  useEffect(() => {
    if (!documentId) {
      setDetails(null);
      setVerificationStatus(null);
      return;
    }

    const fetchData = async () => {
      setLoading(true);
      try {
        // Fetch document details
        const detailsRes = await fetch(`/api/documents/${documentId}`);
        if (detailsRes.ok) {
          const data = await detailsRes.json();
          setDetails(data);
          
          // Initialize field statuses
          const initialStatuses: Record<string, FieldStatus> = {};
          Object.keys(FIELD_LABELS).forEach(key => {
            initialStatuses[key] = { verified: false, edited: false };
          });
          setFieldStatuses(initialStatuses);
        }

        // Check existing verification
        const verifyRes = await fetch(`/api/verify?documentId=${documentId}`);
        if (verifyRes.ok) {
          const verifyData = await verifyRes.json();
          setVerificationStatus(verifyData);
        }
      } catch (error) {
        console.error("Failed to fetch data:", error);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [documentId]);

  const toggleFieldVerified = (field: string) => {
    setFieldStatuses(prev => ({
      ...prev,
      [field]: { ...prev[field], verified: !prev[field]?.verified }
    }));
  };

  const markAllVerified = () => {
    const newStatuses: Record<string, FieldStatus> = {};
    Object.keys(FIELD_LABELS).forEach(key => {
      newStatuses[key] = { ...fieldStatuses[key], verified: true };
    });
    setFieldStatuses(newStatuses);
  };

  const allFieldsVerified = Object.values(fieldStatuses).every(s => s.verified);

  const handleVerify = async () => {
    if (!documentId || !allFieldsVerified) return;
    
    setVerifying(true);
    try {
      const response = await fetch("/api/verify", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          documentId,
          verifiedBy: "Middle Office",
          notes,
          fieldsReviewed: fieldStatuses
        })
      });

      if (response.ok) {
        setShowConfirmDialog(true);
        // Refresh verification status
        const verifyRes = await fetch(`/api/verify?documentId=${documentId}`);
        if (verifyRes.ok) {
          setVerificationStatus(await verifyRes.json());
        }
        onVerificationComplete?.();
      }
    } catch (error) {
      console.error("Verification failed:", error);
    } finally {
      setVerifying(false);
    }
  };

  const formatValue = (key: string, value: unknown): string => {
    if (value === null || value === undefined) return "Not extracted";
    if (typeof value === "boolean") return value ? "Yes" : "No";
    if (key === "CROSS_DEFAULT_THRESHOLD_AMOUNT" && typeof value === "number") {
      return `USD ${value.toLocaleString()}`;
    }
    return String(value);
  };

  if (!documentId) {
    return (
      <div className={cn("flex items-center justify-center h-full", className)}>
        <div className="text-center">
          <ClipboardCheck className="w-16 h-16 mx-auto mb-4 text-slate-300" />
          <p className="text-slate-500">Select a document to verify</p>
        </div>
      </div>
    );
  }

  if (loading) {
    return (
      <div className={cn("p-6 space-y-4", className)}>
        <Skeleton className="h-8 w-64" />
        <Skeleton className="h-24 w-full" />
        <Skeleton className="h-24 w-full" />
        <Skeleton className="h-24 w-full" />
      </div>
    );
  }

  if (!details) {
    return (
      <div className={cn("flex items-center justify-center h-full", className)}>
        <div className="text-center">
          <AlertCircle className="w-16 h-16 mx-auto mb-4 text-amber-400" />
          <p className="text-slate-600">No extraction data available</p>
          <p className="text-sm text-slate-500 mt-1">This document may not have been processed yet</p>
        </div>
      </div>
    );
  }

  const verifiedCount = Object.values(fieldStatuses).filter(s => s.verified).length;
  const totalFields = Object.keys(FIELD_LABELS).length;

  return (
    <ScrollArea className={cn("h-full", className)}>
      <div className="p-6 space-y-6">
        {/* Header */}
        <div className="flex items-start justify-between">
          <div>
            <h2 className="text-xl font-semibold text-slate-900 dark:text-white flex items-center gap-2">
              <FileCheck className="w-6 h-6 text-blue-500" />
              Extraction Verification
            </h2>
            <p className="text-sm text-slate-500 mt-1">
              Review and verify extracted fields from: <span className="font-medium">{fileName}</span>
            </p>
          </div>
          
          {verificationStatus && (
            <Badge className="bg-emerald-100 text-emerald-700 border-emerald-200">
              <CheckCircle2 className="w-3 h-3 mr-1" />
              Verified {new Date(verificationStatus.VERIFIED_AT).toLocaleDateString()}
            </Badge>
          )}
        </div>

        {/* Progress */}
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center justify-between mb-3">
              <span className="text-sm font-medium">Verification Progress</span>
              <span className="text-sm text-slate-500">{verifiedCount} / {totalFields} fields</span>
            </div>
            <div className="h-2 bg-slate-100 dark:bg-slate-800 rounded-full overflow-hidden">
              <div 
                className="h-full bg-gradient-to-r from-blue-500 to-emerald-500 transition-all duration-300"
                style={{ width: `${(verifiedCount / totalFields) * 100}%` }}
              />
            </div>
            <div className="flex justify-end mt-3">
              <Button variant="outline" size="sm" onClick={markAllVerified}>
                <CheckCircle className="w-4 h-4 mr-2" />
                Mark All Verified
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* Fields to Verify */}
        <div className="space-y-3">
          <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-300 uppercase tracking-wider">
            Extracted Fields
          </h3>
          
          {Object.entries(FIELD_LABELS).map(([key, label]) => {
            const value = details[key as keyof DocumentDetails];
            const status = fieldStatuses[key] || { verified: false, edited: false };
            
            return (
              <div 
                key={key}
                className={cn(
                  "flex items-center justify-between p-4 rounded-xl border transition-all",
                  status.verified
                    ? "bg-emerald-50 dark:bg-emerald-950 border-emerald-200 dark:border-emerald-800"
                    : "bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800"
                )}
              >
                <div className="flex-1">
                  <p className="text-xs text-slate-500 uppercase tracking-wider mb-1">{label}</p>
                  <p className={cn(
                    "font-medium",
                    value === null || value === undefined
                      ? "text-slate-400 italic"
                      : "text-slate-900 dark:text-white"
                  )}>
                    {formatValue(key, value)}
                  </p>
                </div>
                
                <Button
                  variant={status.verified ? "default" : "outline"}
                  size="sm"
                  onClick={() => toggleFieldVerified(key)}
                  className={cn(
                    "ml-4",
                    status.verified && "bg-emerald-500 hover:bg-emerald-600 text-white"
                  )}
                >
                  {status.verified ? (
                    <>
                      <CheckCircle2 className="w-4 h-4 mr-1" />
                      Verified
                    </>
                  ) : (
                    <>
                      <CheckCircle className="w-4 h-4 mr-1" />
                      Verify
                    </>
                  )}
                </Button>
              </div>
            );
          })}
        </div>

        {/* Events Summary */}
        {(details.EVENTS_OF_DEFAULT || details.TERMINATION_EVENTS) && (
          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Events Summary</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {details.EVENTS_OF_DEFAULT && (
                <div>
                  <p className="text-xs text-slate-500 mb-2">Events of Default</p>
                  <Badge variant="outline" className="mr-2">
                    {(typeof details.EVENTS_OF_DEFAULT === 'string' 
                      ? JSON.parse(details.EVENTS_OF_DEFAULT) 
                      : details.EVENTS_OF_DEFAULT).length} events extracted
                  </Badge>
                </div>
              )}
              {details.TERMINATION_EVENTS && (
                <div>
                  <p className="text-xs text-slate-500 mb-2">Termination Events</p>
                  <Badge variant="outline">
                    {(typeof details.TERMINATION_EVENTS === 'string'
                      ? JSON.parse(details.TERMINATION_EVENTS)
                      : details.TERMINATION_EVENTS).length} events extracted
                  </Badge>
                </div>
              )}
            </CardContent>
          </Card>
        )}

        {/* Notes */}
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Verification Notes</CardTitle>
          </CardHeader>
          <CardContent>
            <Textarea
              placeholder="Add any notes about this verification (optional)..."
              value={notes}
              onChange={(e) => setNotes(e.target.value)}
              className="min-h-[100px]"
            />
          </CardContent>
        </Card>

        {/* Verify Button */}
        <div className="sticky bottom-0 bg-gradient-to-t from-white dark:from-slate-950 pt-4 pb-2">
          <Button
            onClick={handleVerify}
            disabled={!allFieldsVerified || verifying}
            className={cn(
              "w-full h-12 text-base font-medium shadow-lg",
              allFieldsVerified
                ? "bg-gradient-to-r from-emerald-500 to-teal-500 hover:from-emerald-600 hover:to-teal-600 text-white"
                : "bg-slate-200 text-slate-500"
            )}
          >
            {verifying ? (
              <>
                <Loader2 className="w-5 h-5 mr-2 animate-spin" />
                Submitting Verification...
              </>
            ) : allFieldsVerified ? (
              <>
                <CheckCircle2 className="w-5 h-5 mr-2" />
                Submit Verification
              </>
            ) : (
              <>
                <AlertCircle className="w-5 h-5 mr-2" />
                Verify All Fields to Continue
              </>
            )}
          </Button>
        </div>
      </div>

      {/* Success Dialog */}
      <Dialog open={showConfirmDialog} onOpenChange={setShowConfirmDialog}>
        <DialogContent>
          <DialogHeader>
            <div className="flex items-center justify-center w-16 h-16 mx-auto mb-4 rounded-full bg-emerald-100 dark:bg-emerald-900">
              <CheckCircle2 className="w-8 h-8 text-emerald-500" />
            </div>
            <DialogTitle className="text-center">Verification Complete!</DialogTitle>
            <DialogDescription className="text-center">
              This document has been verified and the verification record has been saved to Snowflake.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button 
              onClick={() => setShowConfirmDialog(false)}
              className="w-full bg-gradient-to-r from-emerald-500 to-teal-500 hover:from-emerald-600 hover:to-teal-600"
            >
              Done
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </ScrollArea>
  );
}

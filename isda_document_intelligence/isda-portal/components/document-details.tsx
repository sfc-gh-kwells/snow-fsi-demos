"use client";

import { useState, useEffect } from "react";
import { 
  FileText, Building2, Calendar, Scale, Shield, AlertTriangle,
  CheckCircle2, XCircle, Loader2
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Separator } from "@/components/ui/separator";
import { Skeleton } from "@/components/ui/skeleton";
import { ScrollArea } from "@/components/ui/scroll-area";
import { cn } from "@/lib/utils";

interface DocumentDetails {
  DOCUMENT_ID: string;
  PARTY_A_NAME: string;
  PARTY_B_NAME: string;
  AGREEMENT_VERSION: string;
  EFFECTIVE_DATE: string;
  GOVERNING_LAW: string;
  EVENTS_OF_DEFAULT: string | Event[];
  TERMINATION_EVENTS: string | Event[];
  CROSS_DEFAULT_APPLICABLE: boolean;
  CROSS_DEFAULT_THRESHOLD_AMOUNT: number;
  CROSS_DEFAULT_THRESHOLD_CURRENCY: string;
  AUTOMATIC_EARLY_TERMINATION_PARTY_A: boolean;
  AUTOMATIC_EARLY_TERMINATION_PARTY_B: boolean;
  CLOSE_OUT_CALCULATION: string;
  SET_OFF_RIGHTS: boolean;
}

interface Event {
  event_type: string;
  grace_period?: string | number;
  triggers_early_termination?: string;
  unwinding_mechanism?: string;
  affected_party?: string;
  waiting_period?: number;
}

interface DocumentDetailsPanelProps {
  documentId: string | null;
  className?: string;
}

export function DocumentDetailsPanel({ documentId, className }: DocumentDetailsPanelProps) {
  const [details, setDetails] = useState<DocumentDetails | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!documentId) {
      setDetails(null);
      return;
    }

    const fetchDetails = async () => {
      setLoading(true);
      try {
        const response = await fetch(`/api/documents/${documentId}`);
        if (response.ok) {
          const data = await response.json();
          setDetails(data);
        }
      } catch (error) {
        console.error("Failed to fetch document details:", error);
      } finally {
        setLoading(false);
      }
    };

    fetchDetails();
  }, [documentId]);

  if (!documentId) {
    return (
      <div className={cn("flex items-center justify-center h-full text-slate-500", className)}>
        <div className="text-center">
          <FileText className="w-12 h-12 mx-auto mb-4 text-slate-300" />
          <p>Select a document to view details</p>
        </div>
      </div>
    );
  }

  if (loading) {
    return (
      <div className={cn("p-6 space-y-6", className)}>
        <Skeleton className="h-8 w-48" />
        <div className="grid grid-cols-2 gap-4">
          <Skeleton className="h-24" />
          <Skeleton className="h-24" />
        </div>
        <Skeleton className="h-32" />
      </div>
    );
  }

  if (!details) {
    return (
      <div className={cn("flex items-center justify-center h-full text-slate-500", className)}>
        <p>Document details not available</p>
      </div>
    );
  }

  const eventsOfDefault: Event[] = details.EVENTS_OF_DEFAULT 
    ? (typeof details.EVENTS_OF_DEFAULT === 'string' 
        ? JSON.parse(details.EVENTS_OF_DEFAULT) 
        : details.EVENTS_OF_DEFAULT)
    : [];
  
  const terminationEvents: Event[] = details.TERMINATION_EVENTS
    ? (typeof details.TERMINATION_EVENTS === 'string'
        ? JSON.parse(details.TERMINATION_EVENTS)
        : details.TERMINATION_EVENTS)
    : [];

  return (
    <ScrollArea className={cn("h-full", className)}>
      <div className="p-6 space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-xl font-semibold text-slate-900 dark:text-white">
              Agreement Details
            </h2>
            <p className="text-sm text-slate-500">
              ISDA {details.AGREEMENT_VERSION} Master Agreement
            </p>
          </div>
          <Badge 
            variant="outline" 
            className={cn(
              "text-sm px-3 py-1",
              details.AGREEMENT_VERSION === "2002" 
                ? "bg-blue-50 text-blue-600 border-blue-200"
                : "bg-amber-50 text-amber-600 border-amber-200"
            )}
          >
            {details.AGREEMENT_VERSION} ISDA
          </Badge>
        </div>

        {/* Parties */}
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium flex items-center gap-2">
              <Building2 className="w-4 h-4 text-slate-500" />
              Counterparties
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 gap-4">
              <div className="p-3 rounded-lg bg-slate-50 dark:bg-slate-900">
                <p className="text-xs text-slate-500 mb-1">Party A</p>
                <p className="font-medium text-slate-900 dark:text-white">
                  {details.PARTY_A_NAME || "N/A"}
                </p>
              </div>
              <div className="p-3 rounded-lg bg-slate-50 dark:bg-slate-900">
                <p className="text-xs text-slate-500 mb-1">Party B</p>
                <p className="font-medium text-slate-900 dark:text-white">
                  {details.PARTY_B_NAME || "N/A"}
                </p>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Key Terms */}
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium flex items-center gap-2">
              <Scale className="w-4 h-4 text-slate-500" />
              Key Terms
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <p className="text-xs text-slate-500 mb-1">Effective Date</p>
                <p className="font-medium">{details.EFFECTIVE_DATE || "N/A"}</p>
              </div>
              <div>
                <p className="text-xs text-slate-500 mb-1">Governing Law</p>
                <p className="font-medium">{details.GOVERNING_LAW || "N/A"}</p>
              </div>
              <div>
                <p className="text-xs text-slate-500 mb-1">Close-out Method</p>
                <p className="font-medium">{details.CLOSE_OUT_CALCULATION || "N/A"}</p>
              </div>
              <div>
                <p className="text-xs text-slate-500 mb-1">Set-off Rights</p>
                <p className="font-medium flex items-center gap-1">
                  {details.SET_OFF_RIGHTS ? (
                    <><CheckCircle2 className="w-4 h-4 text-emerald-500" /> Yes</>
                  ) : (
                    <><XCircle className="w-4 h-4 text-slate-400" /> No</>
                  )}
                </p>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Cross Default */}
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium flex items-center gap-2">
              <Shield className="w-4 h-4 text-slate-500" />
              Cross Default Provisions
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-center gap-4 mb-4">
              <Badge 
                variant={details.CROSS_DEFAULT_APPLICABLE ? "default" : "secondary"}
                className={details.CROSS_DEFAULT_APPLICABLE 
                  ? "bg-emerald-100 text-emerald-700 border-emerald-200"
                  : ""
                }
              >
                {details.CROSS_DEFAULT_APPLICABLE ? "Applicable" : "Not Applicable"}
              </Badge>
            </div>
            {details.CROSS_DEFAULT_APPLICABLE && details.CROSS_DEFAULT_THRESHOLD_AMOUNT && (
              <div className="p-4 rounded-lg bg-slate-50 dark:bg-slate-900">
                <p className="text-xs text-slate-500 mb-1">Threshold Amount</p>
                <p className="text-2xl font-semibold text-slate-900 dark:text-white">
                  {details.CROSS_DEFAULT_THRESHOLD_CURRENCY || "USD"}{" "}
                  {details.CROSS_DEFAULT_THRESHOLD_AMOUNT.toLocaleString()}
                </p>
              </div>
            )}
          </CardContent>
        </Card>

        {/* Automatic Early Termination */}
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium flex items-center gap-2">
              <AlertTriangle className="w-4 h-4 text-slate-500" />
              Automatic Early Termination
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 gap-4">
              <div className={cn(
                "p-4 rounded-lg text-center",
                details.AUTOMATIC_EARLY_TERMINATION_PARTY_A
                  ? "bg-amber-50 dark:bg-amber-950 border border-amber-200 dark:border-amber-800"
                  : "bg-slate-50 dark:bg-slate-900"
              )}>
                <p className="text-xs text-slate-500 mb-2">Party A</p>
                {details.AUTOMATIC_EARLY_TERMINATION_PARTY_A ? (
                  <CheckCircle2 className="w-6 h-6 mx-auto text-amber-500" />
                ) : (
                  <XCircle className="w-6 h-6 mx-auto text-slate-400" />
                )}
                <p className="text-sm font-medium mt-2">
                  {details.AUTOMATIC_EARLY_TERMINATION_PARTY_A ? "Enabled" : "Disabled"}
                </p>
              </div>
              <div className={cn(
                "p-4 rounded-lg text-center",
                details.AUTOMATIC_EARLY_TERMINATION_PARTY_B
                  ? "bg-amber-50 dark:bg-amber-950 border border-amber-200 dark:border-amber-800"
                  : "bg-slate-50 dark:bg-slate-900"
              )}>
                <p className="text-xs text-slate-500 mb-2">Party B</p>
                {details.AUTOMATIC_EARLY_TERMINATION_PARTY_B ? (
                  <CheckCircle2 className="w-6 h-6 mx-auto text-amber-500" />
                ) : (
                  <XCircle className="w-6 h-6 mx-auto text-slate-400" />
                )}
                <p className="text-sm font-medium mt-2">
                  {details.AUTOMATIC_EARLY_TERMINATION_PARTY_B ? "Enabled" : "Disabled"}
                </p>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Events */}
        <Tabs defaultValue="default" className="w-full">
          <TabsList className="w-full">
            <TabsTrigger value="default" className="flex-1">
              Events of Default ({eventsOfDefault.length})
            </TabsTrigger>
            <TabsTrigger value="termination" className="flex-1">
              Termination Events ({terminationEvents.length})
            </TabsTrigger>
          </TabsList>
          
          <TabsContent value="default" className="mt-4">
            <div className="space-y-2">
              {eventsOfDefault.map((event, idx) => (
                <div key={idx} className="p-3 rounded-lg border bg-white dark:bg-slate-900">
                  <div className="flex items-center justify-between">
                    <p className="font-medium text-sm">{event.event_type}</p>
                    <Badge variant="outline" className="text-xs">
                      {event.triggers_early_termination}
                    </Badge>
                  </div>
                  {event.grace_period && (
                    <p className="text-xs text-slate-500 mt-1">
                      Grace period: {event.grace_period}
                    </p>
                  )}
                </div>
              ))}
              {eventsOfDefault.length === 0 && (
                <p className="text-sm text-slate-500 text-center py-4">
                  No events of default extracted
                </p>
              )}
            </div>
          </TabsContent>
          
          <TabsContent value="termination" className="mt-4">
            <div className="space-y-2">
              {terminationEvents.map((event, idx) => (
                <div key={idx} className="p-3 rounded-lg border bg-white dark:bg-slate-900">
                  <div className="flex items-center justify-between">
                    <p className="font-medium text-sm">{event.event_type}</p>
                    <Badge variant="outline" className="text-xs">
                      {event.affected_party}
                    </Badge>
                  </div>
                  {event.waiting_period && (
                    <p className="text-xs text-slate-500 mt-1">
                      Waiting period: {event.waiting_period} days
                    </p>
                  )}
                </div>
              ))}
              {terminationEvents.length === 0 && (
                <p className="text-sm text-slate-500 text-center py-4">
                  No termination events extracted
                </p>
              )}
            </div>
          </TabsContent>
        </Tabs>
      </div>
    </ScrollArea>
  );
}

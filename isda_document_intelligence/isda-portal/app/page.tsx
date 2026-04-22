"use client";

import { useState } from "react";
import { 
  FileText, Library, 
  Bell, Menu, PanelRightClose, PanelRightOpen
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Avatar } from "@/components/ui/avatar";
import { Badge } from "@/components/ui/badge";
import { Sheet, SheetContent, SheetTrigger } from "@/components/ui/sheet";
import { Separator } from "@/components/ui/separator";
import { ChatPanel } from "@/components/chat-panel";
import { DocumentList } from "@/components/document-list";
import { DocumentDetailsPanel } from "@/components/document-details";
import { PDFViewer } from "@/components/pdf-viewer";
import { UploadDialog } from "@/components/upload-dialog";
import { VerificationScreen } from "@/components/verification-screen";
import { cn } from "@/lib/utils";

interface Document {
  DOCUMENT_ID: string;
  FILE_NAME: string;
  DOCUMENT_TYPE: string;
  PARTY_A_NAME: string;
  PARTY_B_NAME: string;
}

export default function Home() {
  const [selectedDocument, setSelectedDocument] = useState<Document | null>(null);
  const [rightTab, setRightTab] = useState<"verify" | "data">("data");
  const [refreshKey, setRefreshKey] = useState(0);
  const [chatOpen, setChatOpen] = useState(true);

  const handleDocumentSelect = (doc: Document) => {
    setSelectedDocument(doc);
  };

  const handleUploadComplete = () => {
    setRefreshKey((k) => k + 1);
  };

  return (
    <div className="flex h-screen bg-slate-100 dark:bg-slate-950">
      {/* Sidebar - Document List */}
      <aside className="hidden lg:flex flex-col w-72 bg-white dark:bg-slate-900 border-r border-slate-200 dark:border-slate-800">
        {/* Logo */}
        <div className="flex items-center gap-3 p-4 border-b border-slate-100 dark:border-slate-800">
          <div className="flex items-center justify-center w-10 h-10 rounded-xl bg-gradient-to-br from-blue-500 to-indigo-600 text-white shadow-lg shadow-blue-500/20">
            <FileText className="w-5 h-5" />
          </div>
          <div>
            <h1 className="font-bold text-slate-900 dark:text-white">ISDA Portal</h1>
            <p className="text-xs text-slate-500">Document Intelligence</p>
          </div>
        </div>

        {/* Upload Button */}
        <div className="p-3 border-b border-slate-100 dark:border-slate-800">
          <UploadDialog onUploadComplete={handleUploadComplete} />
        </div>

        {/* Document List */}
        <div className="flex-1 min-h-0 overflow-hidden">
          <DocumentList
            key={refreshKey}
            selectedId={selectedDocument?.DOCUMENT_ID || null}
            onSelect={handleDocumentSelect}
            className="h-full"
          />
        </div>

        {/* User */}
        <div className="p-4 border-t border-slate-100 dark:border-slate-800">
          <div className="flex items-center gap-3 px-2">
            <Avatar className="w-9 h-9 bg-gradient-to-br from-slate-600 to-slate-700 flex items-center justify-center text-white text-sm font-medium">
              MO
            </Avatar>
            <div className="flex-1 min-w-0">
              <p className="text-sm font-medium text-slate-900 dark:text-white truncate">
                Middle Office
              </p>
              <p className="text-xs text-slate-500 truncate">Operations Team</p>
            </div>
          </div>
        </div>
      </aside>

      {/* Main Content Area */}
      <main className="flex-1 flex flex-col overflow-hidden">
        {/* Top Bar */}
        <header className="flex items-center justify-between px-4 py-3 bg-white dark:bg-slate-900 border-b border-slate-200 dark:border-slate-800">
          {/* Mobile menu */}
          <Sheet>
            <SheetTrigger asChild className="lg:hidden">
              <Button variant="ghost" size="sm">
                <Menu className="w-5 h-5" />
              </Button>
            </SheetTrigger>
            <SheetContent side="left" className="w-72 p-0">
              <div className="flex items-center gap-3 p-4 border-b">
                <div className="flex items-center justify-center w-10 h-10 rounded-xl bg-gradient-to-br from-blue-500 to-indigo-600 text-white">
                  <FileText className="w-5 h-5" />
                </div>
                <div>
                  <h1 className="font-bold">ISDA Portal</h1>
                  <p className="text-xs text-slate-500">Document Intelligence</p>
                </div>
              </div>
              <div className="flex-1 min-h-0 overflow-hidden">
                <DocumentList
                  key={refreshKey}
                  selectedId={selectedDocument?.DOCUMENT_ID || null}
                  onSelect={handleDocumentSelect}
                  className="h-full"
                />
              </div>
            </SheetContent>
          </Sheet>

          <div className="flex items-center gap-3">
            <h2 className="text-lg font-semibold text-slate-900 dark:text-white hidden sm:block">
              Document Verification
            </h2>
            {selectedDocument && (
              <Badge variant="outline" className="hidden md:flex bg-slate-50 dark:bg-slate-800">
                <FileText className="w-3 h-3 mr-1" />
                {selectedDocument.FILE_NAME}
              </Badge>
            )}
          </div>

          <div className="flex items-center gap-2">
            <Button 
              variant="ghost" 
              size="sm" 
              onClick={() => setChatOpen(!chatOpen)}
              className="hidden xl:flex"
            >
              {chatOpen ? (
                <><PanelRightClose className="w-4 h-4 mr-2" /> Hide Assistant</>
              ) : (
                <><PanelRightOpen className="w-4 h-4 mr-2" /> Show Assistant</>
              )}
            </Button>
            <Button variant="ghost" size="sm" className="relative">
              <Bell className="w-5 h-5" />
              <span className="absolute -top-0.5 -right-0.5 w-2 h-2 bg-red-500 rounded-full" />
            </Button>
          </div>
        </header>

        {/* Content Area - Side by Side Layout */}
        <div className="flex-1 flex overflow-hidden">
          {/* PDF Viewer (Left/Center) */}
          <div className="flex-1 min-w-0 bg-slate-50 dark:bg-slate-950 p-4">
            <PDFViewer
              documentId={selectedDocument?.DOCUMENT_ID || null}
              fileName={selectedDocument?.FILE_NAME}
              className="h-full"
            />
          </div>

          {/* Verify/Data Panel (Center/Right) */}
          <div className="w-[450px] border-l border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 flex flex-col">
            <Tabs value={rightTab} onValueChange={(v) => setRightTab(v as "verify" | "data")} className="flex flex-col h-full">
              <TabsList className="mx-4 mt-4 grid grid-cols-2">
                <TabsTrigger value="data" className="flex items-center gap-2">
                  <Library className="w-4 h-4" />
                  Data
                </TabsTrigger>
                <TabsTrigger value="verify" className="flex items-center gap-2">
                  <FileText className="w-4 h-4" />
                  Verify
                </TabsTrigger>
              </TabsList>
              <TabsContent value="data" className="flex-1 overflow-hidden mt-0">
                <DocumentDetailsPanel
                  documentId={selectedDocument?.DOCUMENT_ID || null}
                />
              </TabsContent>
              <TabsContent value="verify" className="flex-1 overflow-hidden mt-0">
                <VerificationScreen
                  documentId={selectedDocument?.DOCUMENT_ID || null}
                  fileName={selectedDocument?.FILE_NAME}
                />
              </TabsContent>
            </Tabs>
          </div>

          {/* Chat Panel (Right) - Collapsible */}
          {chatOpen && (
            <div className="w-[400px] border-l border-slate-200 dark:border-slate-800 hidden xl:flex flex-col bg-white dark:bg-slate-900">
              <ChatPanel />
            </div>
          )}
        </div>
      </main>
    </div>
  );
}

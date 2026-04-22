import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

interface FileResult {
  FILE_NAME: string;
  FILE_PATH: string;
}

interface PresignedUrlResult {
  PRESIGNED_URL: string;
}

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const { searchParams } = new URL(request.url);
    const proxy = searchParams.get("proxy") === "true";
    
    // Get the file name and path from metadata
    const fileResults = await query<FileResult>(`
      SELECT FILE_NAME, FILE_PATH
      FROM ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.RAW_DOCUMENT_METADATA
      WHERE DOCUMENT_ID = '${id}'
    `);
    
    if (fileResults.length === 0) {
      return NextResponse.json({ error: "Document not found" }, { status: 404 });
    }
    
    const { FILE_NAME, FILE_PATH } = fileResults[0];
    
    if (!FILE_NAME || !FILE_NAME.toLowerCase().endsWith('.pdf')) {
      return NextResponse.json({ error: "No PDF available for this document" }, { status: 404 });
    }
    
    console.log("Fetching PDF:", { FILE_NAME, FILE_PATH });
    
    let presignedUrl: string | null = null;
    
    // Handle different FILE_PATH formats
    if (FILE_PATH && FILE_PATH.startsWith('https://')) {
      // Old format - FILE_PATH is already a URL, but it might be expired
      // Try to generate a new presigned URL from the old stage
      const stageName = '@ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.ISDA_DOCUMENTS_STAGE';
      const presignedResults = await query<PresignedUrlResult>(`
        SELECT GET_PRESIGNED_URL(
          ${stageName}, 
          '${FILE_NAME}', 
          3600
        ) AS PRESIGNED_URL
      `);
      
      if (presignedResults.length > 0 && presignedResults[0].PRESIGNED_URL) {
        presignedUrl = presignedResults[0].PRESIGNED_URL;
      }
    } else if (FILE_PATH && FILE_PATH.includes('@')) {
      // New format - FILE_PATH is a stage reference like @STAGE/filename
      // Extract the actual filename from the path
      const stagedFileName = FILE_PATH.split('/').pop() || FILE_NAME;
      
      // Determine which stage to use
      let stageName = '@ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.ISDA_DOCUMENTS_STAGE';
      if (FILE_PATH.includes('ISDA_DOCUMENTS_STAGE')) {
        stageName = '@ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.ISDA_DOCUMENTS_STAGE';
      } else if (FILE_PATH.includes('ISDA_DOCUMENTS')) {
        stageName = '@ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.ISDA_DOCUMENTS';
      }
      
      const presignedResults = await query<PresignedUrlResult>(`
        SELECT GET_PRESIGNED_URL(
          ${stageName}, 
          '${stagedFileName}', 
          3600
        ) AS PRESIGNED_URL
      `);
      
      if (presignedResults.length > 0 && presignedResults[0].PRESIGNED_URL) {
        presignedUrl = presignedResults[0].PRESIGNED_URL;
      }
    } else {
      // Fallback - try stages in order
      const stagesToTry = [
        '@ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.ISDA_DOCUMENTS_STAGE',
        '@ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.ISDA_DOCUMENTS'
      ];
      
      for (const stage of stagesToTry) {
        try {
          const presignedResults = await query<PresignedUrlResult>(`
            SELECT GET_PRESIGNED_URL(
              ${stage}, 
              '${FILE_NAME}', 
              3600
            ) AS PRESIGNED_URL
          `);
          if (presignedResults.length > 0 && presignedResults[0].PRESIGNED_URL) {
            presignedUrl = presignedResults[0].PRESIGNED_URL;
            break;
          }
        } catch {
          // Try next stage
        }
      }
    }
    
    if (!presignedUrl) {
      return NextResponse.json({ error: "Could not generate presigned URL for PDF" }, { status: 500 });
    }
    
    // If proxy mode, fetch the PDF and return it directly
    if (proxy) {
      const pdfResponse = await fetch(presignedUrl);
      if (!pdfResponse.ok) {
        return NextResponse.json({ error: "Failed to fetch PDF from storage" }, { status: 500 });
      }
      
      const pdfBuffer = await pdfResponse.arrayBuffer();
      
      return new NextResponse(pdfBuffer, {
        headers: {
          "Content-Type": "application/pdf",
          "Content-Disposition": `inline; filename="${FILE_NAME}"`,
        },
      });
    }
    
    // Otherwise return the URL (for download link)
    return NextResponse.json({ url: presignedUrl });
  } catch (error) {
    console.error("Error fetching PDF:", error);
    return NextResponse.json({ 
      error: "Failed to fetch PDF",
      details: error instanceof Error ? error.message : "Unknown error"
    }, { status: 500 });
  }
}

import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";
import { writeFile, unlink } from "fs/promises";
import { join } from "path";
import { tmpdir } from "os";
import { randomUUID } from "crypto";

export const maxDuration = 300; // 5 minutes for processing

export async function POST(request: NextRequest) {
  let tempPath: string | null = null;
  
  try {
    const formData = await request.formData();
    const file = formData.get("file") as File;
    const documentType = formData.get("documentType") as string;
    
    if (!file) {
      return NextResponse.json({ error: "No file provided" }, { status: 400 });
    }
    
    if (!documentType) {
      return NextResponse.json({ error: "No document type provided" }, { status: 400 });
    }
    
    console.log(`Processing upload: ${file.name}, type: ${documentType}, size: ${file.size}`);
    
    // Save file temporarily with unique name to avoid conflicts
    const bytes = await file.arrayBuffer();
    const buffer = Buffer.from(bytes);
    const uniqueFileName = `${randomUUID()}_${file.name.replace(/[^a-zA-Z0-9._-]/g, '_')}`;
    tempPath = join(tmpdir(), uniqueFileName);
    await writeFile(tempPath, buffer);
    console.log(`Temp file saved: ${tempPath}`);
    
    const documentId = randomUUID();
    
    // Sanitize file name for SQL (basic protection)
    const safeFileName = file.name.replace(/'/g, "''").replace(/[^\w\s.-]/g, '_');
    
    try {
      // Upload to stage using PUT
      const putCommand = `PUT 'file://${tempPath.replace(/\\/g, '/')}' @ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.ISDA_DOCUMENTS_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE`;
      console.log("Executing PUT command...");
      await query(putCommand);
      console.log("PUT command completed");
    } catch (putError) {
      console.error("PUT command failed:", putError);
      return NextResponse.json({ 
        error: "Failed to upload file to Snowflake stage",
        details: putError instanceof Error ? putError.message : "Unknown error"
      }, { status: 500 });
    }
    
    try {
      // Register in metadata table
      console.log("Registering document metadata...");
      await query(`
        INSERT INTO ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.RAW_DOCUMENT_METADATA (
          DOCUMENT_ID, FILE_NAME, FILE_PATH, DOCUMENT_TYPE, 
          PROCESSING_STATUS, UPLOAD_TIMESTAMP
        )
        VALUES (
          '${documentId}',
          '${safeFileName}',
          '@ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.ISDA_DOCUMENTS_STAGE/${uniqueFileName}',
          '${documentType}',
          'UPLOADED',
          CURRENT_TIMESTAMP()
        )
      `);
      console.log("Metadata registered");
    } catch (metadataError) {
      console.error("Metadata insert failed:", metadataError);
      return NextResponse.json({ 
        error: "Failed to register document metadata",
        details: metadataError instanceof Error ? metadataError.message : "Unknown error"
      }, { status: 500 });
    }
    
    try {
      // Parse document
      console.log("Parsing document...");
      await query(`CALL ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.PARSE_SINGLE_DOCUMENT('${documentId}')`);
      console.log("Document parsed");
    } catch (parseError) {
      console.error("Parse procedure failed:", parseError);
      return NextResponse.json({ 
        success: true,
        documentId,
        warning: "Document uploaded but parsing failed",
        details: parseError instanceof Error ? parseError.message : "Unknown error"
      });
    }
    
    try {
      // Process document (extracts fields, handles amendments, updates knowledge graph)
      console.log("Processing document (extraction + knowledge graph)...");
      const result = await query<{ PROCESS_DOCUMENT: string }>(`CALL ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.PROCESS_DOCUMENT('${documentId}')`);
      const processingResult = result[0]?.PROCESS_DOCUMENT || "Processing completed";
      console.log("Processing result:", processingResult);
      
      return NextResponse.json({ 
        success: true, 
        documentId,
        message: processingResult
      });
    } catch (extractError) {
      console.error("Process procedure failed:", extractError);
      return NextResponse.json({ 
        success: true,
        documentId,
        warning: "Document uploaded and parsed but processing failed",
        details: extractError instanceof Error ? extractError.message : "Unknown error"
      });
    }
    
  } catch (error) {
    console.error("Upload error:", error);
    return NextResponse.json({ 
      error: "Failed to upload document",
      details: error instanceof Error ? error.message : "Unknown error"
    }, { status: 500 });
  } finally {
    // Clean up temp file
    if (tempPath) {
      try {
        await unlink(tempPath);
        console.log("Temp file cleaned up");
      } catch {
        // Ignore cleanup errors
      }
    }
  }
}

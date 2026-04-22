import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

export async function POST(request: NextRequest) {
  try {
    const { documentId, verifiedBy, notes, fieldsReviewed } = await request.json();
    
    if (!documentId) {
      return NextResponse.json({ error: "Document ID required" }, { status: 400 });
    }
    
    await query(`
      INSERT INTO ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.DOCUMENT_VERIFICATIONS (
        DOCUMENT_ID, VERIFIED_BY, NOTES, FIELDS_REVIEWED, VERIFICATION_STATUS
      )
      SELECT
        '${documentId}',
        '${verifiedBy || 'Middle Office'}',
        '${notes || ''}',
        PARSE_JSON('${JSON.stringify(fieldsReviewed || {})}'),
        'VERIFIED'
    `);
    
    return NextResponse.json({ success: true, message: "Document verified successfully" });
  } catch (error) {
    console.error("Verification error:", error);
    return NextResponse.json({ error: "Failed to verify document" }, { status: 500 });
  }
}

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const documentId = searchParams.get("documentId");
    
    if (documentId) {
      const results = await query(`
        SELECT * FROM ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.DOCUMENT_VERIFICATIONS
        WHERE DOCUMENT_ID = '${documentId}'
        ORDER BY VERIFIED_AT DESC
        LIMIT 1
      `);
      return NextResponse.json(results[0] || null);
    }
    
    const results = await query(`
      SELECT v.*, m.FILE_NAME
      FROM ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.DOCUMENT_VERIFICATIONS v
      JOIN ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.RAW_DOCUMENT_METADATA m 
        ON v.DOCUMENT_ID = m.DOCUMENT_ID
      ORDER BY v.VERIFIED_AT DESC
    `);
    return NextResponse.json(results);
  } catch (error) {
    console.error("Fetch verifications error:", error);
    return NextResponse.json({ error: "Failed to fetch verifications" }, { status: 500 });
  }
}

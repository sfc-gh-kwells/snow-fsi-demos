import { NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

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

export async function GET() {
  try {
    const results = await query<Document>(`
      SELECT 
        m.DOCUMENT_ID,
        m.FILE_NAME,
        m.DOCUMENT_TYPE,
        m.UPLOAD_TIMESTAMP,
        m.PROCESSING_STATUS,
        ft.PAGE_COUNT,
        ex.PARTY_A_NAME,
        ex.PARTY_B_NAME,
        ex.AGREEMENT_VERSION,
        ex.EFFECTIVE_DATE,
        ex.GOVERNING_LAW,
        ex.CROSS_DEFAULT_APPLICABLE,
        ex.CROSS_DEFAULT_THRESHOLD_AMOUNT,
        ex.AUTOMATIC_EARLY_TERMINATION_PARTY_A,
        ex.AUTOMATIC_EARLY_TERMINATION_PARTY_B
      FROM ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.RAW_DOCUMENT_METADATA m
      LEFT JOIN ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.DOCUMENT_FULL_TEXT ft 
        ON m.DOCUMENT_ID = ft.DOCUMENT_ID
      LEFT JOIN ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.EXTRACTED_ISDA_MASTER ex 
        ON m.DOCUMENT_ID = ex.DOCUMENT_ID
      ORDER BY m.UPLOAD_TIMESTAMP DESC
    `);
    return NextResponse.json(results);
  } catch (error) {
    console.error("Error fetching documents:", error);
    return NextResponse.json({ error: "Failed to fetch documents" }, { status: 500 });
  }
}

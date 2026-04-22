import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

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

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    
    const results = await query<DocumentDetails>(`
      SELECT 
        e.DOCUMENT_ID,
        m.FILE_NAME,
        e.PARTY_A_NAME,
        e.PARTY_B_NAME,
        e.AGREEMENT_VERSION,
        e.EFFECTIVE_DATE,
        e.GOVERNING_LAW,
        e.EVENTS_OF_DEFAULT,
        e.TERMINATION_EVENTS,
        e.CROSS_DEFAULT_APPLICABLE,
        e.CROSS_DEFAULT_THRESHOLD_AMOUNT,
        e.CROSS_DEFAULT_THRESHOLD_CURRENCY,
        e.AUTOMATIC_EARLY_TERMINATION_PARTY_A,
        e.AUTOMATIC_EARLY_TERMINATION_PARTY_B,
        e.CLOSE_OUT_CALCULATION,
        e.SET_OFF_RIGHTS
      FROM ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.EXTRACTED_ISDA_MASTER e
      LEFT JOIN ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.RAW_DOCUMENT_METADATA m
        ON e.DOCUMENT_ID = m.DOCUMENT_ID
      WHERE e.DOCUMENT_ID = '${id}'
    `);
    
    if (results.length === 0) {
      return NextResponse.json({ error: "Document not found" }, { status: 404 });
    }
    
    return NextResponse.json(results[0]);
  } catch (error) {
    console.error("Error fetching document details:", error);
    return NextResponse.json({ error: "Failed to fetch document details" }, { status: 500 });
  }
}

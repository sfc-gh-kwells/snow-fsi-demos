"""
Synthetic ISDA Document Generator
Generates realistic synthetic PDFs for:
- ISDA Master Agreements (1992 and 2002 versions)
- Credit Support Annexes (VM and IM)
- Master Service Agreements
- Amendments
"""

import os
import random
from datetime import datetime, timedelta
from typing import Dict, List, Any
import json

# Try to import reportlab, provide installation instructions if not available
try:
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
except ImportError:
    print("Please install reportlab: pip install reportlab")
    exit(1)

# Sample data for generating realistic documents
BANKS = [
    ("Goldman Sachs International", "Bank"),
    ("JP Morgan Chase Bank, N.A.", "Bank"),
    ("Morgan Stanley Capital Services LLC", "Bank"),
    ("Citibank, N.A.", "Bank"),
    ("Bank of America, N.A.", "Bank"),
    ("Deutsche Bank AG", "Bank"),
    ("Barclays Bank PLC", "Bank"),
    ("Credit Suisse International", "Bank"),
    ("UBS AG", "Bank"),
    ("HSBC Bank USA, N.A.", "Bank"),
]

COUNTERPARTIES = [
    ("Bridgewater Associates, LP", "Hedge Fund"),
    ("Citadel Advisors LLC", "Hedge Fund"),
    ("Renaissance Technologies LLC", "Hedge Fund"),
    ("Two Sigma Investments, LP", "Hedge Fund"),
    ("AQR Capital Management, LLC", "Hedge Fund"),
    ("BlackRock Financial Management, Inc.", "Asset Manager"),
    ("Pacific Investment Management Company LLC", "Asset Manager"),
    ("Vanguard Group, Inc.", "Asset Manager"),
    ("Fidelity Investments", "Asset Manager"),
    ("State Street Global Advisors", "Asset Manager"),
    ("Microsoft Corporation", "Corporate"),
    ("Apple Inc.", "Corporate"),
    ("Amazon.com, Inc.", "Corporate"),
    ("Tesla, Inc.", "Corporate"),
    ("Exxon Mobil Corporation", "Corporate"),
]

GOVERNING_LAWS = ["New York", "English", "Singapore", "Hong Kong", "Japanese"]

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CHF"]

ELIGIBLE_COLLATERAL = [
    "Cash",
    "U.S. Treasury Securities",
    "U.S. Government Agency Securities",
    "Investment Grade Corporate Bonds",
    "G7 Government Securities",
    "Letters of Credit from Qualified Institutions",
]

EVENTS_OF_DEFAULT = [
    "Failure to Pay or Deliver",
    "Breach of Agreement",
    "Credit Support Default",
    "Misrepresentation",
    "Default Under Specified Transaction",
    "Cross-Default",
    "Bankruptcy",
    "Merger Without Assumption",
]

TERMINATION_EVENTS = [
    "Illegality",
    "Force Majeure",
    "Tax Event",
    "Tax Event Upon Merger",
    "Credit Event Upon Merger",
]


def generate_document_id(doc_type: str) -> str:
    """Generate a unique document ID"""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    random_suffix = random.randint(1000, 9999)
    return f"{doc_type}_{timestamp}_{random_suffix}"


def random_date(start_year: int = 2018, end_year: int = 2024) -> datetime:
    """Generate a random date within range"""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)


def format_currency(amount: float, currency: str = "USD") -> str:
    """Format amount as currency string"""
    symbols = {"USD": "$", "EUR": "€", "GBP": "£", "JPY": "¥", "CHF": "CHF "}
    symbol = symbols.get(currency, currency + " ")
    return f"{symbol}{amount:,.2f}"


class ISDAMasterAgreementGenerator:
    """Generate synthetic ISDA Master Agreement PDFs"""
    
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()
    
    def _setup_custom_styles(self):
        """Setup custom paragraph styles"""
        self.styles.add(ParagraphStyle(
            name='Title_Custom',
            parent=self.styles['Heading1'],
            fontSize=16,
            alignment=TA_CENTER,
            spaceAfter=20,
        ))
        self.styles.add(ParagraphStyle(
            name='SectionHeader',
            parent=self.styles['Heading2'],
            fontSize=12,
            spaceBefore=12,
            spaceAfter=6,
        ))
        self.styles.add(ParagraphStyle(
            name='ClauseText',
            parent=self.styles['Normal'],
            fontSize=10,
            alignment=TA_JUSTIFY,
            spaceBefore=6,
            spaceAfter=6,
            leftIndent=20,
        ))
        self.styles.add(ParagraphStyle(
            name='SubClause',
            parent=self.styles['Normal'],
            fontSize=9,
            alignment=TA_JUSTIFY,
            spaceBefore=3,
            spaceAfter=3,
            leftIndent=40,
        ))
    
    def generate_isda_master(self, party_a: tuple, party_b: tuple, version: str = "2002") -> Dict[str, Any]:
        """Generate an ISDA Master Agreement"""
        doc_id = generate_document_id("ISDA")
        effective_date = random_date()
        governing_law = random.choice(GOVERNING_LAWS)
        
        # Generate cross-default terms
        cross_default_applicable = random.choice([True, True, True, False])  # 75% chance
        cross_default_currency = random.choice(["USD", "EUR", "GBP"])
        cross_default_threshold = random.choice([1000000, 5000000, 10000000, 25000000, 50000000, 100000000])
        
        # Selected events of default
        selected_eods = random.sample(EVENTS_OF_DEFAULT, random.randint(5, 8))
        selected_termination_events = random.sample(TERMINATION_EVENTS, random.randint(3, 5))
        
        # Document metadata
        metadata = {
            "document_id": doc_id,
            "document_type": "ISDA_MASTER",
            "agreement_version": version,
            "effective_date": effective_date.strftime("%Y-%m-%d"),
            "party_a_name": party_a[0],
            "party_a_type": party_a[1],
            "party_b_name": party_b[0],
            "party_b_type": party_b[1],
            "governing_law": governing_law,
            "events_of_default": selected_eods,
            "termination_events": selected_termination_events,
            "cross_default_applicable": cross_default_applicable,
            "cross_default_threshold_amount": cross_default_threshold if cross_default_applicable else None,
            "cross_default_threshold_currency": cross_default_currency if cross_default_applicable else None,
            "netting_applicable": True,
            "close_out_netting": True,
            "specified_entities_party_a": [],
            "specified_entities_party_b": [],
        }
        
        # Generate PDF
        filename = f"{doc_id}.pdf"
        filepath = os.path.join(self.output_dir, filename)
        self._create_isda_pdf(filepath, metadata)
        
        metadata["file_name"] = filename
        metadata["file_path"] = filepath
        
        return metadata
    
    def _create_isda_pdf(self, filepath: str, metadata: Dict[str, Any]):
        """Create the actual ISDA Master Agreement PDF"""
        doc = SimpleDocTemplate(filepath, pagesize=letter,
                               leftMargin=1*inch, rightMargin=1*inch,
                               topMargin=1*inch, bottomMargin=1*inch)
        story = []
        
        # Title
        story.append(Paragraph(
            f"ISDA® {metadata['agreement_version']} MASTER AGREEMENT",
            self.styles['Title_Custom']
        ))
        story.append(Spacer(1, 12))
        
        # Effective date and parties
        story.append(Paragraph(
            f"dated as of {datetime.strptime(metadata['effective_date'], '%Y-%m-%d').strftime('%B %d, %Y')}",
            self.styles['Normal']
        ))
        story.append(Spacer(1, 12))
        
        story.append(Paragraph("between", self.styles['Normal']))
        story.append(Spacer(1, 6))
        
        story.append(Paragraph(
            f"<b>{metadata['party_a_name']}</b> (\"{metadata['party_a_type']}\") (\"Party A\")",
            self.styles['Normal']
        ))
        story.append(Spacer(1, 6))
        story.append(Paragraph("and", self.styles['Normal']))
        story.append(Spacer(1, 6))
        story.append(Paragraph(
            f"<b>{metadata['party_b_name']}</b> (\"{metadata['party_b_type']}\") (\"Party B\")",
            self.styles['Normal']
        ))
        story.append(Spacer(1, 20))
        
        # Section 1 - Interpretation
        story.append(Paragraph("Section 1. Interpretation", self.styles['SectionHeader']))
        story.append(Paragraph(
            "Definitions. The terms defined in Section 14 and elsewhere in this Master Agreement will have the meanings therein specified for the purpose of this Master Agreement.",
            self.styles['ClauseText']
        ))
        
        # Section 2 - Obligations
        story.append(Paragraph("Section 2. Obligations", self.styles['SectionHeader']))
        story.append(Paragraph(
            "(a) General Conditions. Each party will make each payment or delivery specified in each Confirmation to be made by it, subject to the other provisions of this Agreement.",
            self.styles['ClauseText']
        ))
        story.append(Paragraph(
            "(b) Netting of Payments. If on any date amounts would otherwise be payable in the same currency and in respect of the same Transaction by each party to the other, then, on such date, each party's obligation to make payment of any such amount will be automatically satisfied and discharged.",
            self.styles['ClauseText']
        ))
        
        # Section 3 - Representations
        story.append(Paragraph("Section 3. Representations", self.styles['SectionHeader']))
        story.append(Paragraph(
            "Each party makes the representations contained in this Section 3 as of the date of this Agreement and as of each date on which a Transaction is entered into.",
            self.styles['ClauseText']
        ))
        
        # Section 4 - Agreements
        story.append(Paragraph("Section 4. Agreements", self.styles['SectionHeader']))
        story.append(Paragraph(
            "Each party agrees with the other that, so long as either party has or may have any obligation under this Agreement or under any Credit Support Document:",
            self.styles['ClauseText']
        ))
        
        # Section 5 - Events of Default and Termination Events
        story.append(PageBreak())
        story.append(Paragraph("Section 5. Events of Default and Termination Events", self.styles['SectionHeader']))
        
        story.append(Paragraph("(a) Events of Default. The occurrence at any time with respect to a party of any of the following events constitutes an event of default:", self.styles['ClauseText']))
        
        for i, eod in enumerate(metadata['events_of_default'], 1):
            roman = ['(i)', '(ii)', '(iii)', '(iv)', '(v)', '(vi)', '(vii)', '(viii)'][i-1] if i <= 8 else f'({i})'
            story.append(Paragraph(f"{roman} <b>{eod}</b>", self.styles['SubClause']))
        
        # Cross-Default section
        if metadata['cross_default_applicable']:
            story.append(Spacer(1, 12))
            story.append(Paragraph(
                f"<b>Cross-Default:</b> Cross-Default is specified as applying to both Party A and Party B.",
                self.styles['ClauseText']
            ))
            story.append(Paragraph(
                f"<b>Threshold Amount:</b> {format_currency(metadata['cross_default_threshold_amount'], metadata['cross_default_threshold_currency'])} with respect to Party A and Party B.",
                self.styles['SubClause']
            ))
        else:
            story.append(Spacer(1, 12))
            story.append(Paragraph(
                "<b>Cross-Default:</b> Cross-Default is specified as NOT applying.",
                self.styles['ClauseText']
            ))
        
        # Termination Events
        story.append(Spacer(1, 12))
        story.append(Paragraph("(b) Termination Events. The occurrence of any of the following events constitutes a Termination Event:", self.styles['ClauseText']))
        
        for i, te in enumerate(metadata['termination_events'], 1):
            roman = ['(i)', '(ii)', '(iii)', '(iv)', '(v)'][i-1] if i <= 5 else f'({i})'
            story.append(Paragraph(f"{roman} <b>{te}</b>", self.styles['SubClause']))
        
        # Section 6 - Early Termination
        story.append(Paragraph("Section 6. Early Termination; Close-Out Netting", self.styles['SectionHeader']))
        story.append(Paragraph(
            "(a) Right to Terminate Following Event of Default. If at any time an Event of Default with respect to a party has occurred and is then continuing, the other party may, by not more than 20 days notice, designate a day not earlier than the day such notice is effective as an Early Termination Date.",
            self.styles['ClauseText']
        ))
        story.append(Paragraph(
            f"<b>Close-Out Netting:</b> {'Applicable' if metadata['close_out_netting'] else 'Not Applicable'}",
            self.styles['SubClause']
        ))
        
        # Schedule (Part 5)
        story.append(PageBreak())
        story.append(Paragraph("SCHEDULE", self.styles['Title_Custom']))
        story.append(Paragraph("to the", self.styles['Normal']))
        story.append(Paragraph(f"ISDA {metadata['agreement_version']} Master Agreement", self.styles['Normal']))
        story.append(Spacer(1, 20))
        
        # Part 1 - Termination Provisions
        story.append(Paragraph("Part 1. Termination Provisions", self.styles['SectionHeader']))
        
        schedule_data = [
            ["Item", "Party A", "Party B"],
            ["Specified Entity", "None", "None"],
            ["Cross-Default", "Applicable" if metadata['cross_default_applicable'] else "Not Applicable", 
             "Applicable" if metadata['cross_default_applicable'] else "Not Applicable"],
            ["Threshold Amount", 
             format_currency(metadata['cross_default_threshold_amount'], metadata['cross_default_threshold_currency']) if metadata['cross_default_applicable'] else "N/A",
             format_currency(metadata['cross_default_threshold_amount'], metadata['cross_default_threshold_currency']) if metadata['cross_default_applicable'] else "N/A"],
            ["Credit Event Upon Merger", "Applicable", "Applicable"],
        ]
        
        table = Table(schedule_data, colWidths=[2.5*inch, 2*inch, 2*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ]))
        story.append(table)
        
        # Part 4 - Other Provisions
        story.append(Spacer(1, 20))
        story.append(Paragraph("Part 4. Other Provisions", self.styles['SectionHeader']))
        story.append(Paragraph(
            f"<b>Governing Law:</b> This Agreement will be governed by and construed in accordance with the laws of the State of {metadata['governing_law']}.",
            self.styles['ClauseText']
        ))
        
        # Signature block
        story.append(PageBreak())
        story.append(Paragraph("IN WITNESS WHEREOF, the parties have executed this Agreement as of the date first above written.", self.styles['Normal']))
        story.append(Spacer(1, 40))
        
        sig_data = [
            [metadata['party_a_name'], metadata['party_b_name']],
            ["", ""],
            ["_" * 30, "_" * 30],
            ["By: Authorized Signatory", "By: Authorized Signatory"],
            ["Name:", "Name:"],
            ["Title:", "Title:"],
            ["Date:", "Date:"],
        ]
        
        sig_table = Table(sig_data, colWidths=[3*inch, 3*inch])
        sig_table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
        ]))
        story.append(sig_table)
        
        doc.build(story)


class CSAGenerator:
    """Generate synthetic Credit Support Annex PDFs"""
    
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()
    
    def _setup_custom_styles(self):
        """Setup custom paragraph styles"""
        self.styles.add(ParagraphStyle(
            name='Title_Custom',
            parent=self.styles['Heading1'],
            fontSize=14,
            alignment=TA_CENTER,
            spaceAfter=20,
        ))
        self.styles.add(ParagraphStyle(
            name='SectionHeader',
            parent=self.styles['Heading2'],
            fontSize=11,
            spaceBefore=12,
            spaceAfter=6,
        ))
        self.styles.add(ParagraphStyle(
            name='ClauseText',
            parent=self.styles['Normal'],
            fontSize=9,
            alignment=TA_JUSTIFY,
            spaceBefore=6,
            spaceAfter=6,
            leftIndent=20,
        ))
    
    def generate_csa(self, party_a: tuple, party_b: tuple, 
                     parent_isda_id: str = None, csa_type: str = "VM") -> Dict[str, Any]:
        """Generate a Credit Support Annex"""
        doc_id = generate_document_id("CSA")
        effective_date = random_date()
        
        # Generate thresholds (typically in millions)
        currency = random.choice(["USD", "EUR", "GBP"])
        threshold_a = random.choice([0, 1000000, 5000000, 10000000, 25000000, 50000000, float('inf')])
        threshold_b = random.choice([0, 1000000, 5000000, 10000000, 25000000, 50000000, float('inf')])
        
        # Minimum transfer amounts
        mta_a = random.choice([100000, 250000, 500000, 1000000])
        mta_b = random.choice([100000, 250000, 500000, 1000000])
        
        # Independent amounts (Initial Margin)
        ia_a = random.choice([0, 0, 0, 1000000, 5000000, 10000000]) if csa_type == "IM" else 0
        ia_b = random.choice([0, 0, 0, 1000000, 5000000, 10000000]) if csa_type == "IM" else 0
        
        # Eligible collateral
        num_collateral_types = random.randint(2, 5)
        selected_collateral = random.sample(ELIGIBLE_COLLATERAL, num_collateral_types)
        
        # Haircuts
        haircuts = {}
        for coll in selected_collateral:
            if "Cash" in coll:
                haircuts[coll] = 0
            elif "Treasury" in coll or "Government" in coll:
                haircuts[coll] = random.choice([0.5, 1.0, 2.0])
            else:
                haircuts[coll] = random.choice([2.0, 5.0, 8.0, 10.0])
        
        metadata = {
            "document_id": doc_id,
            "document_type": "CSA",
            "csa_type": csa_type,
            "parent_isda_document_id": parent_isda_id,
            "effective_date": effective_date.strftime("%Y-%m-%d"),
            "party_a_name": party_a[0],
            "party_b_name": party_b[0],
            "threshold_party_a": threshold_a if threshold_a != float('inf') else None,
            "threshold_party_a_infinity": threshold_a == float('inf'),
            "threshold_party_a_currency": currency,
            "threshold_party_b": threshold_b if threshold_b != float('inf') else None,
            "threshold_party_b_infinity": threshold_b == float('inf'),
            "threshold_party_b_currency": currency,
            "minimum_transfer_amount_party_a": mta_a,
            "minimum_transfer_amount_party_b": mta_b,
            "mta_currency": currency,
            "independent_amount_party_a": ia_a,
            "independent_amount_party_b": ia_b,
            "eligible_collateral_types": selected_collateral,
            "eligible_currencies": [currency, "USD"] if currency != "USD" else ["USD"],
            "haircuts": haircuts,
            "valuation_agent": random.choice(["Party A", "Party B", "Calculation Agent"]),
            "valuation_frequency": random.choice(["Daily", "Weekly", "Each Local Business Day"]),
            "dispute_resolution_method": "Dispute Resolution Procedure as per Paragraph 5",
            "interest_rate_on_cash_collateral": random.choice(["Fed Funds Rate", "SOFR", "ESTR", "SONIA"]),
        }
        
        # Generate PDF
        filename = f"{doc_id}.pdf"
        filepath = os.path.join(self.output_dir, filename)
        self._create_csa_pdf(filepath, metadata)
        
        metadata["file_name"] = filename
        metadata["file_path"] = filepath
        
        return metadata
    
    def _create_csa_pdf(self, filepath: str, metadata: Dict[str, Any]):
        """Create the actual CSA PDF"""
        doc = SimpleDocTemplate(filepath, pagesize=letter,
                               leftMargin=1*inch, rightMargin=1*inch,
                               topMargin=1*inch, bottomMargin=1*inch)
        story = []
        
        # Title
        csa_title = "CREDIT SUPPORT ANNEX"
        if metadata['csa_type'] == "IM":
            csa_title = "CREDIT SUPPORT ANNEX FOR INITIAL MARGIN (IM)"
        else:
            csa_title = "CREDIT SUPPORT ANNEX FOR VARIATION MARGIN (VM)"
        
        story.append(Paragraph(csa_title, self.styles['Title_Custom']))
        story.append(Paragraph("to the Schedule to the", self.styles['Normal']))
        story.append(Paragraph("ISDA Master Agreement", self.styles['Normal']))
        story.append(Spacer(1, 12))
        
        story.append(Paragraph(
            f"dated as of {datetime.strptime(metadata['effective_date'], '%Y-%m-%d').strftime('%B %d, %Y')}",
            self.styles['Normal']
        ))
        story.append(Spacer(1, 12))
        
        story.append(Paragraph("between", self.styles['Normal']))
        story.append(Paragraph(f"<b>{metadata['party_a_name']}</b> (\"Party A\")", self.styles['Normal']))
        story.append(Paragraph("and", self.styles['Normal']))
        story.append(Paragraph(f"<b>{metadata['party_b_name']}</b> (\"Party B\")", self.styles['Normal']))
        story.append(Spacer(1, 20))
        
        # Paragraph 1 - Interpretation
        story.append(Paragraph("Paragraph 1. Interpretation", self.styles['SectionHeader']))
        story.append(Paragraph(
            "This Annex supplements, forms part of, and is subject to, the ISDA Master Agreement identified above.",
            self.styles['ClauseText']
        ))
        
        # Paragraph 2 - Credit Support Obligations
        story.append(Paragraph("Paragraph 2. Credit Support Obligations", self.styles['SectionHeader']))
        story.append(Paragraph(
            "(a) Delivery Amount. Subject to Paragraphs 4 and 5, upon a demand made by the Secured Party, if the Delivery Amount equals or exceeds the Pledgor's Minimum Transfer Amount, then the Pledgor will transfer Eligible Credit Support having a Value at least equal to the applicable Delivery Amount.",
            self.styles['ClauseText']
        ))
        
        # Paragraph 3 - Transfers, Calculations and Valuations
        story.append(Paragraph("Paragraph 3. Transfers, Calculations and Valuations", self.styles['SectionHeader']))
        story.append(Paragraph(
            f"<b>Valuation Agent:</b> {metadata['valuation_agent']}",
            self.styles['ClauseText']
        ))
        story.append(Paragraph(
            f"<b>Valuation Date:</b> {metadata['valuation_frequency']}",
            self.styles['ClauseText']
        ))
        
        # Paragraph 13 - Elections and Variables
        story.append(PageBreak())
        story.append(Paragraph("Paragraph 13. Elections and Variables", self.styles['SectionHeader']))
        
        # Threshold table
        story.append(Paragraph("<b>(b) Credit Support Amounts:</b>", self.styles['ClauseText']))
        story.append(Spacer(1, 6))
        
        threshold_a_str = "Infinity" if metadata.get('threshold_party_a_infinity') else format_currency(metadata['threshold_party_a'], metadata['threshold_party_a_currency'])
        threshold_b_str = "Infinity" if metadata.get('threshold_party_b_infinity') else format_currency(metadata['threshold_party_b'], metadata['threshold_party_b_currency'])
        
        amounts_data = [
            ["Term", "Party A", "Party B"],
            ["Threshold", threshold_a_str, threshold_b_str],
            ["Minimum Transfer Amount", 
             format_currency(metadata['minimum_transfer_amount_party_a'], metadata['mta_currency']),
             format_currency(metadata['minimum_transfer_amount_party_b'], metadata['mta_currency'])],
            ["Independent Amount", 
             format_currency(metadata['independent_amount_party_a'], metadata['mta_currency']),
             format_currency(metadata['independent_amount_party_b'], metadata['mta_currency'])],
        ]
        
        table = Table(amounts_data, colWidths=[2.5*inch, 2*inch, 2*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.lightgrey),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ]))
        story.append(table)
        story.append(Spacer(1, 20))
        
        # Eligible Collateral table
        story.append(Paragraph("<b>(c) Eligible Collateral:</b>", self.styles['ClauseText']))
        story.append(Spacer(1, 6))
        
        collateral_data = [["Collateral Type", "Valuation Percentage (Haircut)"]]
        for coll, haircut in metadata['haircuts'].items():
            collateral_data.append([coll, f"{100 - haircut}% (Haircut: {haircut}%)"])
        
        coll_table = Table(collateral_data, colWidths=[3.5*inch, 2.5*inch])
        coll_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.lightgrey),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ]))
        story.append(coll_table)
        story.append(Spacer(1, 20))
        
        # Other terms
        story.append(Paragraph(
            f"<b>(d) Eligible Currency:</b> {', '.join(metadata['eligible_currencies'])}",
            self.styles['ClauseText']
        ))
        story.append(Paragraph(
            f"<b>(e) Interest Rate:</b> {metadata['interest_rate_on_cash_collateral']}",
            self.styles['ClauseText']
        ))
        story.append(Paragraph(
            f"<b>(f) Dispute Resolution:</b> {metadata['dispute_resolution_method']}",
            self.styles['ClauseText']
        ))
        
        doc.build(story)


class AmendmentGenerator:
    """Generate synthetic Amendment PDFs"""
    
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()
    
    def _setup_custom_styles(self):
        """Setup custom paragraph styles"""
        self.styles.add(ParagraphStyle(
            name='Title_Custom',
            parent=self.styles['Heading1'],
            fontSize=14,
            alignment=TA_CENTER,
            spaceAfter=20,
        ))
        self.styles.add(ParagraphStyle(
            name='SectionHeader',
            parent=self.styles['Heading2'],
            fontSize=11,
            spaceBefore=12,
            spaceAfter=6,
        ))
        self.styles.add(ParagraphStyle(
            name='ClauseText',
            parent=self.styles['Normal'],
            fontSize=9,
            alignment=TA_JUSTIFY,
            spaceBefore=6,
            spaceAfter=6,
            leftIndent=20,
        ))
    
    def generate_amendment(self, party_a: tuple, party_b: tuple,
                          parent_document_id: str, parent_document_type: str,
                          amendment_number: int = 1) -> Dict[str, Any]:
        """Generate an Amendment document"""
        doc_id = generate_document_id("AMEND")
        amendment_date = random_date(2022, 2024)
        effective_date = amendment_date + timedelta(days=random.randint(0, 30))
        
        # Generate modifications based on parent document type
        if parent_document_type == "CSA":
            modifications = self._generate_csa_modifications()
            clauses_modified = list(modifications.keys())
        else:  # ISDA_MASTER
            modifications = self._generate_isda_modifications()
            clauses_modified = list(modifications.keys())
        
        metadata = {
            "document_id": doc_id,
            "document_type": "AMENDMENT",
            "parent_document_id": parent_document_id,
            "parent_document_type": parent_document_type,
            "amendment_number": amendment_number,
            "amendment_date": amendment_date.strftime("%Y-%m-%d"),
            "effective_date": effective_date.strftime("%Y-%m-%d"),
            "party_a_name": party_a[0],
            "party_b_name": party_b[0],
            "clauses_modified": clauses_modified,
            "modifications": modifications,
            "superseded_terms": {k: f"Previous {k} value" for k in modifications.keys()},
            "reason_for_amendment": random.choice([
                "Annual review and update of credit terms",
                "Regulatory compliance update",
                "Change in counterparty credit rating",
                "Mutual agreement to modify terms",
                "Update to reflect market conditions",
            ]),
        }
        
        # Generate PDF
        filename = f"{doc_id}.pdf"
        filepath = os.path.join(self.output_dir, filename)
        self._create_amendment_pdf(filepath, metadata)
        
        metadata["file_name"] = filename
        metadata["file_path"] = filepath
        
        return metadata
    
    def _generate_csa_modifications(self) -> Dict[str, Any]:
        """Generate random CSA modifications"""
        possible_mods = {
            "Threshold (Party A)": format_currency(random.choice([5000000, 10000000, 25000000]), "USD"),
            "Threshold (Party B)": format_currency(random.choice([5000000, 10000000, 25000000]), "USD"),
            "Minimum Transfer Amount": format_currency(random.choice([250000, 500000, 1000000]), "USD"),
            "Valuation Frequency": random.choice(["Daily", "Each Local Business Day"]),
            "Interest Rate": random.choice(["SOFR + 0.25%", "SOFR", "Fed Funds Rate"]),
            "Eligible Collateral": "Added: Investment Grade Corporate Bonds rated A or higher",
        }
        # Select 1-3 random modifications
        num_mods = random.randint(1, 3)
        selected_keys = random.sample(list(possible_mods.keys()), num_mods)
        return {k: possible_mods[k] for k in selected_keys}
    
    def _generate_isda_modifications(self) -> Dict[str, Any]:
        """Generate random ISDA modifications"""
        possible_mods = {
            "Cross-Default Threshold Amount": format_currency(random.choice([10000000, 25000000, 50000000]), "USD"),
            "Specified Entities (Party A)": random.choice(["None", "All Affiliates", "Material Subsidiaries"]),
            "Specified Entities (Party B)": random.choice(["None", "All Affiliates", "Material Subsidiaries"]),
            "Credit Event Upon Merger": random.choice(["Applicable", "Not Applicable"]),
            "Additional Termination Event": "Added: Ratings Downgrade below BBB-",
        }
        num_mods = random.randint(1, 3)
        selected_keys = random.sample(list(possible_mods.keys()), num_mods)
        return {k: possible_mods[k] for k in selected_keys}
    
    def _create_amendment_pdf(self, filepath: str, metadata: Dict[str, Any]):
        """Create the actual Amendment PDF"""
        doc = SimpleDocTemplate(filepath, pagesize=letter,
                               leftMargin=1*inch, rightMargin=1*inch,
                               topMargin=1*inch, bottomMargin=1*inch)
        story = []
        
        # Title
        ordinal = {1: "First", 2: "Second", 3: "Third", 4: "Fourth", 5: "Fifth"}.get(metadata['amendment_number'], f"{metadata['amendment_number']}th")
        
        if metadata['parent_document_type'] == "CSA":
            title = f"{ordinal.upper()} AMENDMENT TO CREDIT SUPPORT ANNEX"
        else:
            title = f"{ordinal.upper()} AMENDMENT TO ISDA MASTER AGREEMENT"
        
        story.append(Paragraph(title, self.styles['Title_Custom']))
        story.append(Spacer(1, 12))
        
        story.append(Paragraph(
            f"Amendment Date: {datetime.strptime(metadata['amendment_date'], '%Y-%m-%d').strftime('%B %d, %Y')}",
            self.styles['Normal']
        ))
        story.append(Paragraph(
            f"Effective Date: {datetime.strptime(metadata['effective_date'], '%Y-%m-%d').strftime('%B %d, %Y')}",
            self.styles['Normal']
        ))
        story.append(Spacer(1, 12))
        
        story.append(Paragraph("between", self.styles['Normal']))
        story.append(Paragraph(f"<b>{metadata['party_a_name']}</b> (\"Party A\")", self.styles['Normal']))
        story.append(Paragraph("and", self.styles['Normal']))
        story.append(Paragraph(f"<b>{metadata['party_b_name']}</b> (\"Party B\")", self.styles['Normal']))
        story.append(Spacer(1, 20))
        
        # Recitals
        story.append(Paragraph("RECITALS", self.styles['SectionHeader']))
        story.append(Paragraph(
            f"WHEREAS, Party A and Party B have previously entered into an agreement (the \"Original Agreement\", Document ID: {metadata['parent_document_id']});",
            self.styles['ClauseText']
        ))
        story.append(Paragraph(
            f"WHEREAS, the parties wish to amend certain terms of the Original Agreement;",
            self.styles['ClauseText']
        ))
        story.append(Paragraph(
            f"WHEREAS, the reason for this amendment is: {metadata['reason_for_amendment']};",
            self.styles['ClauseText']
        ))
        story.append(Spacer(1, 12))
        
        # Amendments
        story.append(Paragraph("AMENDMENTS", self.styles['SectionHeader']))
        story.append(Paragraph(
            "NOW, THEREFORE, in consideration of the mutual agreements contained herein, the parties agree as follows:",
            self.styles['ClauseText']
        ))
        story.append(Spacer(1, 6))
        
        for i, (clause, new_value) in enumerate(metadata['modifications'].items(), 1):
            story.append(Paragraph(
                f"<b>{i}. {clause}</b>",
                self.styles['ClauseText']
            ))
            story.append(Paragraph(
                f"The {clause} provision is hereby amended and restated in its entirety as follows:",
                self.styles['ClauseText']
            ))
            story.append(Paragraph(
                f"<b>New Value:</b> {new_value}",
                self.styles['ClauseText']
            ))
            story.append(Spacer(1, 6))
        
        # Effect of Amendment
        story.append(Spacer(1, 12))
        story.append(Paragraph("EFFECT OF AMENDMENT", self.styles['SectionHeader']))
        story.append(Paragraph(
            "Except as expressly amended hereby, the Original Agreement shall remain in full force and effect. In the event of any conflict between the terms of this Amendment and the Original Agreement, the terms of this Amendment shall prevail.",
            self.styles['ClauseText']
        ))
        
        # Signature block
        story.append(Spacer(1, 30))
        story.append(Paragraph(
            "IN WITNESS WHEREOF, the parties have executed this Amendment as of the date first above written.",
            self.styles['Normal']
        ))
        story.append(Spacer(1, 20))
        
        sig_data = [
            [metadata['party_a_name'], metadata['party_b_name']],
            ["_" * 25, "_" * 25],
            ["By: Authorized Signatory", "By: Authorized Signatory"],
        ]
        
        sig_table = Table(sig_data, colWidths=[3*inch, 3*inch])
        sig_table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
        ]))
        story.append(sig_table)
        
        doc.build(story)


class MSAGenerator:
    """Generate synthetic Master Service Agreement PDFs"""
    
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()
    
    def _setup_custom_styles(self):
        """Setup custom paragraph styles"""
        self.styles.add(ParagraphStyle(
            name='Title_Custom',
            parent=self.styles['Heading1'],
            fontSize=14,
            alignment=TA_CENTER,
            spaceAfter=20,
        ))
        self.styles.add(ParagraphStyle(
            name='SectionHeader',
            parent=self.styles['Heading2'],
            fontSize=11,
            spaceBefore=12,
            spaceAfter=6,
        ))
        self.styles.add(ParagraphStyle(
            name='ClauseText',
            parent=self.styles['Normal'],
            fontSize=9,
            alignment=TA_JUSTIFY,
            spaceBefore=6,
            spaceAfter=6,
            leftIndent=20,
        ))
    
    def generate_msa(self, service_provider: tuple, client: tuple) -> Dict[str, Any]:
        """Generate a Master Service Agreement"""
        doc_id = generate_document_id("MSA")
        effective_date = random_date()
        
        term_months = random.choice([12, 24, 36, 60])
        fee_type = random.choice(["Fixed", "Variable", "Tiered"])
        fee_currency = random.choice(["USD", "EUR", "GBP"])
        
        metadata = {
            "document_id": doc_id,
            "document_type": "MSA",
            "effective_date": effective_date.strftime("%Y-%m-%d"),
            "service_provider_name": service_provider[0],
            "service_provider_type": service_provider[1],
            "client_name": client[0],
            "client_type": client[1],
            "term_length_months": term_months,
            "auto_renewal": random.choice([True, False]),
            "termination_notice_days": random.choice([30, 60, 90]),
            "termination_for_convenience": random.choice([True, False]),
            "termination_for_cause_events": [
                "Material Breach",
                "Insolvency",
                "Failure to Perform",
            ],
            "fee_structure_type": fee_type,
            "fee_amount": random.choice([50000, 100000, 250000, 500000, 1000000]),
            "fee_currency": fee_currency,
            "sla_terms": {
                "availability": "99.9%",
                "response_time": "4 hours",
                "resolution_time": "24 hours",
            },
            "governing_law": random.choice(GOVERNING_LAWS),
        }
        
        # Generate PDF
        filename = f"{doc_id}.pdf"
        filepath = os.path.join(self.output_dir, filename)
        self._create_msa_pdf(filepath, metadata)
        
        metadata["file_name"] = filename
        metadata["file_path"] = filepath
        
        return metadata
    
    def _create_msa_pdf(self, filepath: str, metadata: Dict[str, Any]):
        """Create the actual MSA PDF"""
        doc = SimpleDocTemplate(filepath, pagesize=letter,
                               leftMargin=1*inch, rightMargin=1*inch,
                               topMargin=1*inch, bottomMargin=1*inch)
        story = []
        
        story.append(Paragraph("MASTER SERVICE AGREEMENT", self.styles['Title_Custom']))
        story.append(Spacer(1, 12))
        
        story.append(Paragraph(
            f"Effective Date: {datetime.strptime(metadata['effective_date'], '%Y-%m-%d').strftime('%B %d, %Y')}",
            self.styles['Normal']
        ))
        story.append(Spacer(1, 12))
        
        story.append(Paragraph("between", self.styles['Normal']))
        story.append(Paragraph(
            f"<b>{metadata['service_provider_name']}</b> (\"{metadata['service_provider_type']}\") (\"Service Provider\")",
            self.styles['Normal']
        ))
        story.append(Paragraph("and", self.styles['Normal']))
        story.append(Paragraph(
            f"<b>{metadata['client_name']}</b> (\"{metadata['client_type']}\") (\"Client\")",
            self.styles['Normal']
        ))
        story.append(Spacer(1, 20))
        
        # Term
        story.append(Paragraph("1. TERM", self.styles['SectionHeader']))
        story.append(Paragraph(
            f"This Agreement shall commence on the Effective Date and continue for a period of {metadata['term_length_months']} months.",
            self.styles['ClauseText']
        ))
        if metadata['auto_renewal']:
            story.append(Paragraph(
                "This Agreement shall automatically renew for successive one-year periods unless either party provides written notice of non-renewal.",
                self.styles['ClauseText']
            ))
        
        # Fees
        story.append(Paragraph("2. FEES AND PAYMENT", self.styles['SectionHeader']))
        story.append(Paragraph(
            f"<b>Fee Structure:</b> {metadata['fee_structure_type']}",
            self.styles['ClauseText']
        ))
        story.append(Paragraph(
            f"<b>Annual Fee:</b> {format_currency(metadata['fee_amount'], metadata['fee_currency'])}",
            self.styles['ClauseText']
        ))
        
        # Termination
        story.append(Paragraph("3. TERMINATION", self.styles['SectionHeader']))
        story.append(Paragraph(
            f"<b>Notice Period:</b> {metadata['termination_notice_days']} days written notice",
            self.styles['ClauseText']
        ))
        story.append(Paragraph(
            f"<b>Termination for Convenience:</b> {'Permitted' if metadata['termination_for_convenience'] else 'Not Permitted'}",
            self.styles['ClauseText']
        ))
        story.append(Paragraph("<b>Termination for Cause Events:</b>", self.styles['ClauseText']))
        for event in metadata['termination_for_cause_events']:
            story.append(Paragraph(f"• {event}", self.styles['ClauseText']))
        
        # SLA
        story.append(Paragraph("4. SERVICE LEVEL AGREEMENT", self.styles['SectionHeader']))
        for term, value in metadata['sla_terms'].items():
            story.append(Paragraph(f"<b>{term.replace('_', ' ').title()}:</b> {value}", self.styles['ClauseText']))
        
        # Governing Law
        story.append(Paragraph("5. GOVERNING LAW", self.styles['SectionHeader']))
        story.append(Paragraph(
            f"This Agreement shall be governed by and construed in accordance with the laws of {metadata['governing_law']}.",
            self.styles['ClauseText']
        ))
        
        doc.build(story)


def generate_document_set(output_dir: str, num_relationships: int = 5) -> List[Dict[str, Any]]:
    """Generate a complete set of related documents"""
    all_documents = []
    
    isda_gen = ISDAMasterAgreementGenerator(output_dir)
    csa_gen = CSAGenerator(output_dir)
    amend_gen = AmendmentGenerator(output_dir)
    msa_gen = MSAGenerator(output_dir)
    
    # Generate counterparty relationships
    for i in range(num_relationships):
        bank = random.choice(BANKS)
        counterparty = random.choice(COUNTERPARTIES)
        
        # Generate ISDA Master Agreement
        isda_version = random.choice(["1992", "2002"])
        isda = isda_gen.generate_isda_master(bank, counterparty, isda_version)
        all_documents.append(isda)
        print(f"Generated ISDA Master: {isda['document_id']}")
        
        # Generate CSA (80% chance)
        if random.random() < 0.8:
            csa_type = random.choice(["VM", "IM"])
            csa = csa_gen.generate_csa(bank, counterparty, isda['document_id'], csa_type)
            all_documents.append(csa)
            print(f"Generated CSA: {csa['document_id']}")
            
            # Generate CSA amendment (40% chance)
            if random.random() < 0.4:
                amend = amend_gen.generate_amendment(bank, counterparty, csa['document_id'], "CSA", 1)
                all_documents.append(amend)
                print(f"Generated Amendment: {amend['document_id']}")
                
                # Second amendment (20% chance)
                if random.random() < 0.2:
                    amend2 = amend_gen.generate_amendment(bank, counterparty, csa['document_id'], "CSA", 2)
                    all_documents.append(amend2)
                    print(f"Generated Amendment: {amend2['document_id']}")
        
        # Generate MSA (30% chance)
        if random.random() < 0.3:
            msa = msa_gen.generate_msa(bank, counterparty)
            all_documents.append(msa)
            print(f"Generated MSA: {msa['document_id']}")
    
    # Save metadata to JSON
    metadata_file = os.path.join(output_dir, "document_metadata.json")
    with open(metadata_file, 'w') as f:
        json.dump(all_documents, f, indent=2, default=str)
    print(f"\nMetadata saved to: {metadata_file}")
    
    return all_documents


if __name__ == "__main__":
    output_directory = "./synthetic_isda_documents"
    print(f"Generating synthetic ISDA documents to: {output_directory}")
    docs = generate_document_set(output_directory, num_relationships=5)
    print(f"\nGenerated {len(docs)} documents total")

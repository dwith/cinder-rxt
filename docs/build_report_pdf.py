#!/usr/bin/env python3
import os
import re
import subprocess
import sys

DOCS_DIR = os.path.dirname(os.path.abspath(__file__))
REPORT_MD = os.path.join(DOCS_DIR, "cinder_rxt_performance_report.md")
HTML_OUT = os.path.join(DOCS_DIR, "cinder_rxt_performance_report.html")
PDF_OUT = os.path.join(DOCS_DIR, "cinder_rxt_performance_report.pdf")
ARTIFACTS_DIR = "/Users/Dan.With/.gemini/antigravity-ide/brain/7058a561-4b47-4df2-9ede-d1c663b15020"

def build_report_pdf():
    print(f"Reading markdown from {REPORT_MD}...")
    with open(REPORT_MD, "r", encoding="utf-8") as f:
        md_content = f.read()

    # Pre-process callouts in markdown
    # Convert [!NOTE] -> <div class="callout callout-note">...
    def callout_sub(match):
        ctype = match.group(1).lower()
        title = match.group(1).upper()
        content = match.group(2).strip()
        return f'\n\n::: callout-{ctype}\n**{title}:** {content}\n:::\n\n'

    # Pandoc conversion
    pandoc_proc = subprocess.run(
        ["/opt/homebrew/bin/pandoc", "-f", "markdown", "-t", "html"],
        input=md_content,
        text=True,
        capture_output=True,
        check=True
    )
    body_html = pandoc_proc.stdout

    # Transform callout blockquotes
    body_html = re.sub(
        r'<blockquote>\s*<p>\[!NOTE\]\s*(.*?)</p>\s*</blockquote>',
        r'<div class="callout callout-note"><div class="callout-title"><span class="callout-icon">ℹ️</span> NOTE</div><div class="callout-content"><p>\1</p></div></div>',
        body_html,
        flags=re.DOTALL
    )
    body_html = re.sub(
        r'<blockquote>\s*<p>\[!IMPORTANT\]\s*(.*?)</p>\s*</blockquote>',
        r'<div class="callout callout-important"><div class="callout-title"><span class="callout-icon">⭐</span> IMPORTANT</div><div class="callout-content"><p>\1</p></div></div>',
        body_html,
        flags=re.DOTALL
    )
    body_html = re.sub(
        r'<blockquote>\s*<p>\[!WARNING\]\s*(.*?)</p>\s*</blockquote>',
        r'<div class="callout callout-warning"><div class="callout-title"><span class="callout-icon">⚠️</span> WARNING</div><div class="callout-content"><p>\1</p></div></div>',
        body_html,
        flags=re.DOTALL
    )
    body_html = re.sub(
        r'<blockquote>\s*<p>\[!TIP\]\s*(.*?)</p>\s*</blockquote>',
        r'<div class="callout callout-tip"><div class="callout-title"><span class="callout-icon">💡</span> TIP</div><div class="callout-content"><p>\1</p></div></div>',
        body_html,
        flags=re.DOTALL
    )

    full_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>OpenStack Cinder: cinder-rxt Driver Certification & Performance Report</title>
  <style>
    @page {{
      size: letter portrait;
      margin: 18mm 16mm 20mm 16mm;
      @bottom-left {{
        content: "Rackspace OpenStack Genestack 2026.3.1 | cinder-rxt Certification Report";
        font-size: 8pt;
        color: #64748b;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      }}
      @bottom-right {{
        content: "Page " counter(page);
        font-size: 8pt;
        font-weight: 600;
        color: #475569;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      }}
    }}

    *, *:before, *:after {{
      box-sizing: border-box;
    }}

    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      font-size: 10pt;
      line-height: 1.55;
      color: #1e293b;
      background: #ffffff;
      margin: 0;
      padding: 0;
    }}

    .header-banner {{
      border-bottom: 2px solid #0284c7;
      padding-bottom: 12px;
      margin-bottom: 20px;
    }}

    .badge-container {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 8px;
      margin-bottom: 16px;
    }}

    .badge {{
      display: inline-block;
      padding: 3px 8px;
      border-radius: 4px;
      font-size: 8pt;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }}

    .badge-success {{
      background: #ecfdf5;
      color: #047857;
      border: 1px solid #a7f3d0;
    }}

    .badge-info {{
      background: #f0f9ff;
      color: #0369a1;
      border: 1px solid #bae6fd;
    }}

    .badge-purple {{
      background: #faf5ff;
      color: #6b21a8;
      border: 1px solid #e9d5ff;
    }}

    h1 {{
      font-size: 19pt;
      font-weight: 800;
      color: #0f172a;
      letter-spacing: -0.5px;
      margin-top: 0;
      margin-bottom: 8px;
      line-height: 1.25;
    }}

    h2 {{
      font-size: 14pt;
      font-weight: 700;
      color: #0f172a;
      border-bottom: 1px solid #e2e8f0;
      padding-bottom: 6px;
      margin-top: 24px;
      margin-bottom: 12px;
      page-break-after: avoid;
    }}

    h3 {{
      font-size: 11pt;
      font-weight: 700;
      color: #1e293b;
      margin-top: 18px;
      margin-bottom: 8px;
      page-break-after: avoid;
    }}

    h4 {{
      font-size: 10pt;
      font-weight: 600;
      color: #334155;
      margin-top: 14px;
      margin-bottom: 6px;
      page-break-after: avoid;
    }}

    p {{
      margin-top: 0;
      margin-bottom: 10px;
    }}

    ul, ol {{
      margin-top: 0;
      margin-bottom: 10px;
      padding-left: 20px;
    }}

    li {{
      margin-bottom: 4px;
    }}

    code {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
      font-size: 8.5pt;
      background: #f1f5f9;
      color: #0f172a;
      padding: 2px 4px;
      border-radius: 4px;
      border: 1px solid #e2e8f0;
    }}

    pre {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
      font-size: 8pt;
      line-height: 1.45;
      background: #0f172a;
      color: #f8fafc;
      padding: 12px 14px;
      border-radius: 6px;
      overflow-x: auto;
      margin: 12px 0;
      page-break-inside: avoid;
    }}

    pre code {{
      background: transparent;
      color: inherit;
      padding: 0;
      border: none;
      font-size: 8pt;
    }}

    table {{
      width: 100%;
      border-collapse: collapse;
      margin: 14px 0;
      font-size: 8.5pt;
      line-height: 1.4;
      page-break-inside: avoid;
    }}

    th, td {{
      padding: 7px 10px;
      border: 1px solid #cbd5e1;
      text-align: left;
      vertical-align: top;
    }}

    th {{
      background-color: #1e293b;
      color: #ffffff;
      font-weight: 600;
    }}

    tr:nth-child(even) td {{
      background-color: #f8fafc;
    }}

    a {{
      color: #0284c7;
      text-decoration: none;
    }}

    hr {{
      border: none;
      border-top: 1px solid #e2e8f0;
      margin: 20px 0;
    }}

    .callout {{
      border-left: 4px solid #0284c7;
      background: #f0f9ff;
      padding: 10px 14px;
      border-radius: 0 6px 6px 0;
      margin: 14px 0;
      page-break-inside: avoid;
    }}

    .callout-title {{
      font-weight: 700;
      font-size: 8.5pt;
      margin-bottom: 4px;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      display: flex;
      align-items: center;
      gap: 6px;
    }}

    .callout-content p:last-child {{
      margin-bottom: 0;
    }}

    .callout-note {{
      border-color: #0284c7;
      background: #f0f9ff;
      color: #0c4a6e;
    }}
    .callout-note .callout-title {{ color: #0284c7; }}

    .callout-important {{
      border-color: #7c3aed;
      background: #faf5ff;
      color: #4c1d95;
    }}
    .callout-important .callout-title {{ color: #7c3aed; }}

    .callout-warning {{
      border-color: #d97706;
      background: #fffbeb;
      color: #78350f;
    }}
    .callout-warning .callout-title {{ color: #d97706; }}

    .callout-tip {{
      border-color: #059669;
      background: #ecfdf5;
      color: #064e3b;
    }}
    .callout-tip .callout-title {{ color: #059669; }}
  </style>
</head>
<body>

  <div class="header-banner">
    <div class="badge-container">
      <span class="badge badge-success">✓ Certification Complete</span>
      <span class="badge badge-info">OpenStack Genestack 2026.3.1</span>
      <span class="badge badge-purple">OSPC-2358 Verified Fixed</span>
      <span class="badge badge-info">DFW-DEV Environment</span>
    </div>
  </div>

  {body_html}

</body>
</html>
"""

    with open(HTML_OUT, "w", encoding="utf-8") as f:
        f.write(full_html)
    print(f"Wrote styled HTML to {HTML_OUT}")

    # Generate PDF using headless Chrome
    print(f"Generating PDF via Google Chrome headless...")
    chrome_cmd = [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "--headless",
        "--disable-gpu",
        "--no-pdf-header-footer",
        f"--print-to-pdf={PDF_OUT}",
        HTML_OUT
    ]
    subprocess.run(chrome_cmd, check=True)
    print(f"Generated PDF: {PDF_OUT} ({os.path.getsize(PDF_OUT)} bytes)")

    # Copy to artifacts directory
    artifact_pdf = os.path.join(ARTIFACTS_DIR, "cinder_rxt_performance_report.pdf")
    subprocess.run(["cp", PDF_OUT, artifact_pdf], check=True)
    print(f"Copied PDF to artifacts: {artifact_pdf}")

if __name__ == "__main__":
    build_report_pdf()

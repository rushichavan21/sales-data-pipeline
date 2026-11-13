"""Generate an architecture diagram image for the Sales Data Pipeline."""

import os
from PIL import Image, ImageDraw, ImageFont

def draw_rounded_rect(draw, coords, radius, fill, outline, width=2):
    x0, y0, x1, y1 = coords
    draw.rounded_rectangle([x0, y0, x1, y1], radius=radius, fill=fill, outline=outline, width=width)

def main():
    width = 1200
    height = 630
    img = Image.new("RGB", (width, height), color="#0F172A")  # Deep slate dark background
    draw = ImageDraw.Draw(img)

    # Use default bitmap font
    font = ImageFont.load_default()

    # Title Banner
    draw.text((40, 30), "SALES DATA PIPELINE ARCHITECTURE", fill="#F8FAFC", font=font)
    draw.text((40, 50), "End-to-End Local-First Cloud Data Engineering Pipeline", fill="#94A3B8", font=font)

    # Stage Boxes
    stages = [
        {
            "title": "1. Ingestion & Raw Data",
            "subtitle": "Superstore Retail CSV (~30k rows)",
            "box": (40, 110, 260, 420),
            "color": "#1E293B",
            "border": "#38BDF8",
            "items": ["• Synthetic Generator", "• Messy data injection", "• Local: data/raw/", "• S3: s3://bucket/raw/"]
        },
        {
            "title": "2. PySpark Cleaning & DQ",
            "subtitle": "Local & AWS Glue (Portable)",
            "box": (280, 110, 540, 420),
            "color": "#1E293B",
            "border": "#818CF8",
            "items": ["• Schema enforcement", "• Multi-format dates", "• Deduplication", "• DQ Assertions & Metrics", "• Partition: Year / Month"]
        },
        {
            "title": "3. Storage & Athena",
            "subtitle": "Curated S3 & Serverless SQL",
            "box": (560, 110, 840, 420),
            "color": "#1E293B",
            "border": "#34D399",
            "items": ["• Parquet + Snappy", "• 80.8% storage saving", "• Athena External DDL", "• 5 Analytical SQL Queries", "• Partition pruning"]
        },
        {
            "title": "4. Warehouse & dbt",
            "subtitle": "Snowflake & Star Schema",
            "box": (860, 110, 1160, 420),
            "color": "#1E293B",
            "border": "#F472B6",
            "items": ["• Snowflake X-Small (60s suspend)", "• Staging views: stg_sales", "• Star Schema: fact_sales", "• Dimensions: date, cust, prod", "• dbt tests: unique, rels"]
        }
    ]

    for stage in stages:
        draw_rounded_rect(draw, stage["box"], radius=12, fill=stage["color"], outline=stage["border"], width=3)
        bx0, by0, bx1, by1 = stage["box"]
        draw.text((bx0 + 15, by0 + 20), stage["title"], fill="#F8FAFC", font=font)
        draw.text((bx0 + 15, by0 + 40), stage["subtitle"], fill=stage["border"], font=font)

        line_y = by0 + 80
        for item in stage["items"]:
            draw.text((bx0 + 15, line_y), item, fill="#E2E8F0", font=font)
            line_y += 35

    # Connecting Flow Arrows / Indicators
    arrow_color = "#38BDF8"
    draw.text((263, 260), "==>", fill=arrow_color, font=font)
    draw.text((543, 260), "==>", fill=arrow_color, font=font)
    draw.text((843, 260), "==>", fill=arrow_color, font=font)

    # Bottom Cost & Guardrails bar
    draw_rounded_rect(draw, (40, 450, 1160, 580), radius=10, fill="#1E293B", outline="#F59E0B", width=2)
    draw.text((60, 470), "COST CONTROL & PRODUCTION GUARDRAILS (< $10 BUDGET TARGET)", fill="#FBBF24", font=font)
    draw.text((60, 500), "• Local-First Testing: All PySpark and unit tests run 100% locally with zero cloud spend during development.", fill="#CBD5E1", font=font)
    draw.text((60, 525), "• AWS Glue: Flex execution + 2 G.1X workers + 10m safety timeout (~$0.03/run). Upfront cost estimation.", fill="#CBD5E1", font=font)
    draw.text((60, 550), "• Snowflake: X-Small Warehouse with auto_suspend = 60s and automated teardown scripts.", fill="#CBD5E1", font=font)

    os.makedirs("docs", exist_ok=True)
    out_path = os.path.join("docs", "architecture.png")
    img.save(out_path)
    print(f"Architecture diagram generated: {out_path}")

if __name__ == "__main__":
    main()

"""Generates the small sample files used by `make minio-init`.

Real files, not renamed text: the PDFs open in a PDF viewer, the .docx in Word
and the .xlsx in Excel. Standard library only. Output: scripts/minio/samples/.
Run again only to change the samples; the generated files are committed.
"""
import os
import zipfile

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "samples")


def pdf(path, title, lines):
    """A one-page PDF with a title and text lines (Helvetica, ASCII)."""
    text = [f"BT /F1 16 Tf 50 790 Td ({title}) Tj ET"]
    y = 760
    for line in lines:
        text.append(f"BT /F1 11 Tf 50 {y} Td ({line}) Tj ET")
        y -= 16
    stream = "\n".join(text).encode()

    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Contents 4 0 R "
        b"/Resources << /Font << /F1 5 0 R >> >> >>",
        b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for i, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{i} 0 obj\n".encode() + body + b"\nendobj\n"
    xref = len(out)
    out += f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode()
    for off in offsets:
        out += f"{off:010d} 00000 n \n".encode()
    out += f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    with open(path, "wb") as f:
        f.write(out)


def docx(path, title, paragraphs):
    def p(text, bold=False):
        run_props = "<w:rPr><w:b/></w:rPr>" if bold else ""
        return f"<w:p><w:r>{run_props}<w:t xml:space=\"preserve\">{text}</w:t></w:r></w:p>"

    body = p(title, bold=True) + "".join(p(t) for t in paragraphs)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml",
                   '<?xml version="1.0" encoding="UTF-8"?>'
                   '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                   '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
                   '<Default Extension="xml" ContentType="application/xml"/>'
                   '<Override PartName="/word/document.xml" '
                   'ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>'
                   '</Types>')
        z.writestr("_rels/.rels",
                   '<?xml version="1.0" encoding="UTF-8"?>'
                   '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                   '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
                   'Target="word/document.xml"/></Relationships>')
        z.writestr("word/document.xml",
                   '<?xml version="1.0" encoding="UTF-8"?>'
                   '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
                   f'<w:body>{body}</w:body></w:document>')


def xlsx(path, rows):
    def cell(ref, value):
        if isinstance(value, int):
            return f'<c r="{ref}"><v>{value}</v></c>'
        return f'<c r="{ref}" t="inlineStr"><is><t>{value}</t></is></c>'

    sheet_rows = []
    for r, row in enumerate(rows, start=1):
        cells = "".join(cell(f"{chr(65 + c)}{r}", v) for c, v in enumerate(row))
        sheet_rows.append(f'<row r="{r}">{cells}</row>')

    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml",
                   '<?xml version="1.0" encoding="UTF-8"?>'
                   '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                   '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
                   '<Default Extension="xml" ContentType="application/xml"/>'
                   '<Override PartName="/xl/workbook.xml" '
                   'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
                   '<Override PartName="/xl/worksheets/sheet1.xml" '
                   'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
                   '</Types>')
        z.writestr("_rels/.rels",
                   '<?xml version="1.0" encoding="UTF-8"?>'
                   '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                   '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
                   'Target="xl/workbook.xml"/></Relationships>')
        z.writestr("xl/workbook.xml",
                   '<?xml version="1.0" encoding="UTF-8"?>'
                   '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
                   'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
                   '<sheets><sheet name="Summary" sheetId="1" r:id="rId1"/></sheets></workbook>')
        z.writestr("xl/_rels/workbook.xml.rels",
                   '<?xml version="1.0" encoding="UTF-8"?>'
                   '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                   '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
                   'Target="worksheets/sheet1.xml"/></Relationships>')
        z.writestr("xl/worksheets/sheet1.xml",
                   '<?xml version="1.0" encoding="UTF-8"?>'
                   '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                   f'<sheetData>{"".join(sheet_rows)}</sheetData></worksheet>')


def main():
    os.makedirs(OUT, exist_ok=True)
    pdf(os.path.join(OUT, "registry-a.pdf"), "Registry A (sample, synthetic data)", [
        "1. Ivanov Ivan, IIN 000000000001",
        "2. Petrova Anna, IIN 000000000002",
        "3. Sidorov Oleg, IIN 000000000003",
    ])
    pdf(os.path.join(OUT, "registry-b.pdf"), "Registry B (sample, synthetic data)", [
        "1. Ivanov Ivan, IIN 000000000001",
        "2. Petrova Anna, IIN 000000000002",
        "3. Akhmetov Daniyar, IIN 000000000004",
    ])
    docx(os.path.join(OUT, "report.docx"), "Comparison report (sample)", [
        "This is a sample report from the mock AI. Synthetic data.",
        "In both lists: 2. Only in A: 1. Only in B: 1. Needs review: 0.",
    ])
    xlsx(os.path.join(OUT, "report.xlsx"), [
        ["Category", "Count"],
        ["both", 2],
        ["only_a", 1],
        ["only_b", 1],
        ["review", 0],
    ])
    for name in sorted(os.listdir(OUT)):
        print(f"{name}: {os.path.getsize(os.path.join(OUT, name))} bytes")


if __name__ == "__main__":
    main()

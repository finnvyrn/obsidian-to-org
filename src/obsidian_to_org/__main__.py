#!/usr/bin/env python3

import argparse
import pathlib
import re
import subprocess
import tempfile
import uuid
import shutil
from concurrent.futures import ProcessPoolExecutor

COMMENT_MARKER = "#!#comment:"
RULER_RE = re.compile(r"^---\n(.+)", re.MULTILINE)
EMBED_RE = re.compile(r"!\[\[([^\]|]+)(?:\|[^\]]*)?\]\]")
WIKI_DESC_RE = re.compile(r"\[\[([^\]|]+)\|([^\]]+)\]\]")
WIKI_SIMPLE_RE = re.compile(r"\[\[([^\]|\[]+)\]\]")
FILE_LINK_RE = re.compile(r"\[\[file:(.*?)\]\[(.*?)\]\]")
BARE_LINK_RE = re.compile(r"\[\[([^\]|\[]+)\]\]")
END_QUOTE_FIX_RE = re.compile(r"\n[ \t]*\n(\#\+end_quote)")
# match lines beginning with "> >"
DOUBLE_BQ_RE = re.compile(r"^> >\s?(.*)$", re.MULTILINE)


def fix_double_blockquotes(md):
    """
    Transform any Markdown line starting with "> > text"
    into '> "text"'
    """
    return DOUBLE_BQ_RE.sub(r'> "\1"', md)


def fix_markdown_comments(markdown_contents):
    chunks = markdown_contents.split("%%")
    inside = False
    out = []
    for chunk in chunks:
        if not inside:
            out.append(chunk)
            inside = True
        else:
            if "\n" in chunk:
                lines = chunk.splitlines(True)
                if lines and lines[0].strip() == "":
                    lines = lines[1:]
                out.extend(f"{COMMENT_MARKER}{l}" for l in lines)
            else:
                out.extend(["<!--", chunk, "-->"])
            inside = False
    return "".join(out)


def restore_comments(org_contents):
    return "".join(
        line.replace(COMMENT_MARKER, "# ") for line in org_contents.splitlines(True)
    )


def prepare_markdown_text(md):
    # first normalize double blockquotes
    md = fix_double_blockquotes(md)
    # then convert our custom comment markers
    md = fix_markdown_comments(md)
    # ensure pandoc sees a blank line after any --- ruler
    return RULER_RE.sub(r"---\n\n\1", md)


def fix_links(org):
    # 0) convert embeds: ![[name|size]] → [[file:../attachments/name]]
    def embed_repl(m):
        fname = m.group(1)
        return f"[[file:../attachments/{fname}]]"

    org = EMBED_RE.sub(embed_repl, org)

    # 1) convert wiki [[page|desc]] → [[file:page.org][desc]]
    org = WIKI_DESC_RE.sub(lambda m: f"[[file:{m.group(1)}.org][{m.group(2)}]]", org)

    # 2) convert wiki [[page]] → [[file:page.org][page]], but skip URLs/paths
    def simple_repl(m):
        page = m.group(1)
        if re.match(r"https?://|.*/|.*\..*", page):
            return m.group(0)
        return f"[[file:{page}.org][{page}]]"

    org = WIKI_SIMPLE_RE.sub(simple_repl, org)

    return org


def fix_list_indent(text):
    """
    Double the leading spaces on every list-item line:
    so 2 → 4, 4 → 8, etc., preserving nested levels.
    """

    def repl(m):
        orig = m.group(1)
        new_indent = " " * (len(orig) * 2)
        return f"{new_indent}- "

    return re.sub(r"^([ \t]*)- ", repl, text, flags=re.MULTILINE)


def fix_attribution_space(text):
    """
    Ensure any line starting with “―” has a space after it.
    """
    return re.sub(r"^―(?! )", "― ", text, flags=re.MULTILINE)


def convert_markdown_file(md_file, org_file):
    # 1) Markdown → Org via pandoc
    contents = prepare_markdown_text(md_file.read_text())
    with tempfile.NamedTemporaryFile("w+") as tmp:
        tmp.write(contents)
        tmp.flush()
        subprocess.run(
            [
                "pandoc",
                "--from=markdown-auto_identifiers",
                "--to=org",
                "--wrap=preserve",
                "--output",
                str(org_file),
                tmp.name,
            ],
            check=True,
        )

    # 2) Read back and post-process
    org = org_file.read_text()
    org = restore_comments(org)
    org = fix_links(org)
    org = END_QUOTE_FIX_RE.sub(r"\n\1", org)
    org = fix_list_indent(org)
    org = fix_attribution_space(org)
    org_file.write_text(org)


def add_node_id(org_file, node_id):
    content = org_file.read_text()
    with org_file.open("w") as f:
        f.write(":PROPERTIES:\n")
        f.write(f":ID: {node_id}\n")
        f.write(":END:\n")
        f.write(f"#+title: {org_file.stem}\n\n")
        f.write(content)


def walk_directory(path):
    for p in path.iterdir():
        if p.is_dir():
            yield from walk_directory(p)
        else:
            yield p.resolve()


def worker_convert(args):
    md_path, org_path, node_id = args
    convert_markdown_file(md_path, org_path)
    add_node_id(org_path, node_id)
    return md_path.name


def convert_directory():
    parser = argparse.ArgumentParser()
    parser.add_argument("markdown_directory", type=pathlib.Path)
    parser.add_argument("output_directory", type=pathlib.Path)
    args = parser.parse_args()

    in_dir = args.markdown_directory.resolve()
    out_dir = args.output_directory
    out_dir.mkdir(parents=True, exist_ok=True)

    # assign each .md a UUID
    nodes = {}
    jobs = []
    for path in walk_directory(in_dir):
        if path.name == ".DS_Store":
            continue
        if path.suffix != ".md":
            tgt = out_dir / path.relative_to(in_dir)
            tgt.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(path, tgt)
            continue

        org_rel = path.relative_to(in_dir).with_suffix(".org")
        org_full = out_dir / org_rel
        org_full.parent.mkdir(parents=True, exist_ok=True)

        nid = str(uuid.uuid4()).upper()
        key = re.sub(r"[\u00A0\s]+", " ", org_rel.stem).strip()
        nodes[key] = nid

        jobs.append((path, org_full, nid))

    # run conversions in parallel
    with ProcessPoolExecutor() as exe:
        for fname in exe.map(worker_convert, jobs):
            print(f"Converted {fname}")

    # then fix up file→id links
    for org_path in walk_directory(out_dir):
        if org_path.suffix != ".org":
            continue
        text = org_path.read_text()

        # 1) file:… → id:…
        def repl_file(m):
            raw, label = m.group(1), m.group(2)
            label = re.sub(r"[\u00A0]", " ", label)
            stem = pathlib.Path(raw).stem
            norm = re.sub(r"[\u00A0\s]+", " ", stem).strip()
            nid = nodes.get(norm)
            return f"[[id:{nid}][{label}]]" if nid else m.group(0)

        text = FILE_LINK_RE.sub(repl_file, text)

        # 2) bare [[Page]]
        def repl_bare(m):
            name_norm = re.sub(r"[\u00A0]", " ", m.group(1))
            key = name_norm.strip()
            nid = nodes.get(key)
            return f"[[id:{nid}][{name_norm}]]" if nid else m.group(0)

        text = BARE_LINK_RE.sub(repl_bare, text)

        org_path.write_text(text)
        print(f"Fixed links in {org_path}")


if __name__ == "__main__":
    convert_directory()

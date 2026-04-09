"""Codebase reading utilities for ALI.

Provides targeted file reading so ALI can include relevant source
code in her context window without loading the entire codebase.
The file map and project root are passed in by the caller —
this module is project-agnostic.
"""
from pathlib import Path


def get_relevant_files(category, file_map, base_path,
                       affected_pages=None):
    """Determine which source files are relevant given a category.

    :param category: Category string (e.g., 'reporting', 'plan').
    :param file_map: Dict mapping category strings to lists of
        glob patterns relative to base_path. Example::

            {
                'reporting': [
                    'app/features/processor_analysis.py',
                    'processor/reporting/*.py',
                ],
            }

    :param base_path: Absolute path to the project root.
    :param affected_pages: Optional list of additional category
        keys to include files from.
    :returns: List of absolute Path objects that exist on disk.
    """
    base_path = Path(base_path)
    patterns = list(file_map.get(category, []))

    if affected_pages:
        for page in affected_pages:
            patterns.extend(file_map.get(page, []))

    patterns = list(dict.fromkeys(patterns))

    resolved = []
    for pattern in patterns:
        if '*' in pattern:
            resolved.extend(base_path.glob(pattern))
        else:
            p = base_path / pattern
            if p.exists():
                resolved.append(p)

    return list(dict.fromkeys(resolved))


def read_file_with_limit(filepath, max_lines=500):
    """Read a source file, truncating if too long.

    If the file exceeds max_lines, includes the first half and
    last half with a truncation notice in between.

    :param filepath: Path to the file.
    :param max_lines: Maximum lines to include.
    :returns: String with a ``# FILE:`` header and file contents,
        or None if the file can't be read.
    """
    try:
        filepath = Path(filepath)
        if not filepath.exists():
            return None

        lines = filepath.read_text(encoding='utf-8').splitlines()
        rel_path = str(filepath.name)

        if len(lines) <= max_lines:
            content = '\n'.join(lines)
        else:
            half = max_lines // 2
            head = '\n'.join(lines[:half])
            tail = '\n'.join(lines[-half:])
            omitted = len(lines) - max_lines
            content = (
                f"{head}\n\n"
                f"# ... ({omitted} lines omitted) ...\n\n"
                f"{tail}"
            )

        return f"# FILE: {rel_path}\n{content}"

    except Exception:
        return None


def build_code_context(category, file_map, base_path,
                       doc_files=None, affected_pages=None,
                       max_total_chars=50000):
    """Build a combined context string of relevant source files.

    Main entry point for context assembly. Given a category and
    file map, reads matching files within a character budget.

    :param category: Category string for file targeting.
    :param file_map: Dict mapping categories to file glob patterns.
    :param base_path: Project root path.
    :param doc_files: Optional list of absolute paths to
        documentation files to include first.
    :param affected_pages: Optional list of additional category
        keys.
    :param max_total_chars: Character budget for combined output.
    :returns: Combined context string with file contents separated
        by ``---`` dividers.
    """
    sections = []
    total_chars = 0

    if doc_files:
        for doc_path in doc_files:
            content = read_file_with_limit(doc_path, max_lines=300)
            if content and total_chars + len(content) < max_total_chars:
                sections.append(content)
                total_chars += len(content)

    for src_path in get_relevant_files(
            category, file_map, base_path, affected_pages):
        content = read_file_with_limit(src_path, max_lines=200)
        if content and total_chars + len(content) < max_total_chars:
            sections.append(content)
            total_chars += len(content)

    return '\n\n---\n\n'.join(sections)


def extract_function(filepath, function_name, max_lines=200):
    """Extract a single function from a source file.

    Reads from the ``def function_name`` line through the end of
    the function body (detected by indentation returning to the
    function's level or a new top-level definition).

    :param filepath: Path to the source file.
    :param function_name: Name of the function to extract.
    :param max_lines: Safety limit on function length.
    :returns: Tuple of (function_text, start_line, end_line),
        or (None, None, None) if not found.
    """
    try:
        filepath = Path(filepath)
        lines = filepath.read_text(encoding='utf-8').splitlines()

        start = None
        base_indent = None
        for i, line in enumerate(lines):
            stripped = line.lstrip()
            if stripped.startswith(f'def {function_name}('):
                start = i
                base_indent = len(line) - len(stripped)
                break

        if start is None:
            return None, None, None

        end = start + 1
        for i in range(start + 1,
                       min(start + max_lines, len(lines))):
            line = lines[i]
            if not line.strip():
                end = i + 1
                continue
            current_indent = len(line) - len(line.lstrip())
            if current_indent <= base_indent and line.strip():
                stripped = line.lstrip()
                if (stripped.startswith('def ')
                        or stripped.startswith('class ')
                        or stripped.startswith('@')):
                    break
            end = i + 1

        function_text = '\n'.join(lines[start:end])
        return function_text, start + 1, end

    except Exception:
        return None, None, None


def get_file_summary(filepath, max_lines=30):
    """Return just the docstring/header of a file for reference.

    :param filepath: Path to the file.
    :param max_lines: Fallback line limit if no docstring boundary
        found.
    :returns: String with the file header, or None on failure.
    """
    try:
        filepath = Path(filepath)
        lines = filepath.read_text(encoding='utf-8').splitlines()
        in_docstring = False
        for i, line in enumerate(lines[:max_lines]):
            if '"""' in line or "'''" in line:
                if in_docstring:
                    return '\n'.join(lines[:i + 1])
                in_docstring = True
        return '\n'.join(lines[:max_lines])
    except Exception:
        return None


def get_context_for_query(message, file_map, base_path,
                          doc_files=None, area_keywords=None,
                          max_total_chars=15000):
    """Determine if a chat message benefits from source context.

    Heuristic: if the message mentions feature areas AND contains
    technical signal words, build a targeted context.

    :param message: The user's chat message.
    :param file_map: Category-to-files mapping.
    :param base_path: Project root path.
    :param doc_files: Optional doc file paths.
    :param area_keywords: Dict mapping keyword strings to category
        strings. If None, returns None.
    :param max_total_chars: Character budget for context.
    :returns: Code context string if relevant, None if not needed.
    """
    if not area_keywords:
        return None

    matched_category = None
    message_lower = message.lower()
    for keyword, category in area_keywords.items():
        if keyword in message_lower:
            matched_category = category
            break

    if not matched_category:
        return None

    technical_signals = [
        'how does', 'how do', 'where is', 'which file',
        'what function', 'how to', 'why does', 'code',
        'implement', 'fix', 'bug', 'error', 'broken',
        'work', 'function', 'class', 'method', 'route',
        'module', 'file', 'logic', 'call', 'what is',
        'explain', 'show me', 'find', 'source', 'defined',
    ]
    if not any(signal in message_lower for signal in technical_signals):
        return None

    return build_code_context(
        matched_category,
        file_map=file_map,
        base_path=base_path,
        doc_files=doc_files,
        max_total_chars=max_total_chars,
    )

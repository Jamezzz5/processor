"""Code generation utilities for ALI.

Given a system prompt, codebase context, and a task description,
generates implementation files. The caller (app layer) is responsible
for loading task details, building prompts, and storing results.
"""
import re


def generate_code(system_prompt, task_description, code_context,
                  llm_call_fn, previous_generation=None,
                  review_notes=None, additional_instructions=None):
    """Generate implementation code for a task.

    :param system_prompt: System prompt defining coding conventions
        and output format. Provided by the app layer.
    :param task_description: Structured description of what to build.
    :param code_context: Source code and documentation context string.
    :param llm_call_fn: Callable ``(system_prompt, user_prompt)``
        returning a string. Provided by the app layer so LLM
        config stays in Flask.
    :param previous_generation: Optional dict from a prior
        generation (files, explanation) for iteration.
    :param review_notes: Optional review feedback to incorporate.
    :param additional_instructions: Optional extra guidance.
    :returns: Dict with ``files``, ``explanation``,
        ``implementation_prompt``, or None on failure.
    """
    user_prompt = _build_user_prompt(
        task_description, code_context,
        previous_generation, review_notes,
        additional_instructions,
    )

    response = llm_call_fn(system_prompt, user_prompt)
    if not response:
        return None

    return parse_code_response(response)


def parse_code_response(response_text):
    """Parse an LLM response into structured files.

    Extracts ``FILE:`` blocks, ``IMPLEMENTATION_NOTES:``, and
    ``ENGINEER_PROMPT:`` sections.

    :param response_text: Raw LLM output.
    :returns: Dict with ``files``, ``explanation``,
        ``implementation_prompt``.
    """
    files = _extract_file_blocks(response_text)
    explanation = _extract_section(
        response_text, 'IMPLEMENTATION_NOTES:')
    engineer_prompt = _extract_section(
        response_text, 'ENGINEER_PROMPT:')

    return {
        'files': files,
        'explanation': explanation,
        'implementation_prompt': engineer_prompt,
    }


def _build_user_prompt(task_description, code_context,
                       previous_generation=None,
                       review_notes=None,
                       additional_instructions=None):
    """Assemble the user-role prompt for code generation.

    :param task_description: What to build/fix.
    :param code_context: Relevant source code and docs.
    :param previous_generation: Prior attempt for iteration.
    :param review_notes: Feedback on prior attempt.
    :param additional_instructions: Extra guidance.
    :returns: Formatted prompt string.
    """
    parts = [
        f"Task:\n{task_description}",
        f"\nCodebase context:\n{code_context}",
    ]

    if previous_generation:
        prev_files = previous_generation.get('files', [])
        prev_summary = '\n'.join(
            f"- {f['path']}: {f.get('description', '')}"
            for f in prev_files
        )
        parts.append(
            f"\nPrevious attempt produced these files:"
            f"\n{prev_summary}"
            f"\n\nPrevious explanation:\n"
            f"{previous_generation.get('explanation', '')}"
        )

    if review_notes:
        parts.append(
            f"\nReview feedback to address:\n{review_notes}")

    if additional_instructions:
        parts.append(
            f"\nAdditional instructions:\n"
            f"{additional_instructions}")

    return '\n'.join(parts)


def _extract_file_blocks(text):
    """Extract ``FILE: path`` / code blocks from LLM output.

    :param text: Raw LLM response.
    :returns: List of dicts with ``path``, ``content``,
        ``description``.
    """
    blocks = []
    pattern = re.compile(
        r'FILE:\s*(.+?)\s*\n'
        r'(?:DESCRIPTION:\s*(.+?)\s*\n)?'
        r'```\w*\n(.*?)```',
        re.DOTALL,
    )

    for match in pattern.finditer(text):
        blocks.append({
            'path': match.group(1).strip(),
            'description': (match.group(2) or '').strip(),
            'content': match.group(3).strip(),
        })

    return blocks


def _extract_section(text, header):
    """Extract a named section from LLM output.

    :param text: Raw LLM response.
    :param header: Section header to find.
    :returns: Section content string, or empty string.
    """
    if header not in text:
        return ''

    start = text.index(header) + len(header)
    boundaries = [
        'FILE:', 'IMPLEMENTATION_NOTES:', 'ENGINEER_PROMPT:',
    ]
    end = len(text)
    for boundary in boundaries:
        if boundary == header:
            continue
        idx = text.find(boundary, start)
        if idx != -1 and idx < end:
            end = idx

    return text[start:end].strip()

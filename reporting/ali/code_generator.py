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
        ``implementation_prompt`` and the raw answer under ``raw``,
        or None on failure. ``raw`` rides along because the app layer
        discards an unusable generation and would otherwise have
        nothing left to say what the model sent.
    """
    user_prompt = _build_user_prompt(
        task_description, code_context,
        previous_generation, review_notes,
        additional_instructions,
    )

    response = llm_call_fn(system_prompt, user_prompt)
    if not response:
        return None

    parsed = parse_code_response(response)
    parsed['raw'] = response
    return parsed


def parse_code_response(response_text):
    """Parse an LLM response into structured files.

    Extracts ``FILE:`` blocks (full contents), ``PATCH:`` blocks
    (anchored function replacements), ``IMPLEMENTATION_NOTES:``,
    and ``ENGINEER_PROMPT:`` sections.

    ``dropped`` names every block header found but not turned into a
    file, which is how the caller tells a model that never used the
    format from one that tried and hit something specific — two
    outcomes calling for opposite responses. See :func:`describe_parse`.

    :param response_text: Raw LLM output.
    :returns: Dict with ``files``, ``patches``, ``dropped``,
        ``explanation``, ``implementation_prompt``.
    """
    files, patches, dropped = [], [], []
    for block in _scan_blocks(response_text):
        if block['problem']:
            dropped.append({'kind': block['kind'],
                            'path': block['path'],
                            'problem': block['problem']})
        elif block['kind'] == 'PATCH':
            patches.append(block)
        else:
            files.append(block)
    return {
        'files': files,
        'patches': patches,
        'dropped': dropped,
        'explanation': _extract_section(
            response_text, 'IMPLEMENTATION_NOTES'),
        'implementation_prompt': _extract_section(
            response_text, 'ENGINEER_PROMPT'),
    }


def describe_parse(result, response_text=''):
    """One plain sentence saying what the model's answer amounted to.

    The generation lane discards an unparseable answer, so this is
    the whole record of a night's work that produced nothing. It
    distinguishes the three outcomes needing different fixes: the
    format was ignored, it was used but the block was unusable, or
    the answer was cut off mid-block.

    :param result: a :func:`parse_code_response` dict.
    :param response_text: the raw answer, for the length note.
    :returns: a short human-readable diagnosis.
    """
    kept = len(result.get('files') or []) + len(
        result.get('patches') or [])
    dropped = result.get('dropped') or []
    if kept:
        return '{} usable block(s), {} dropped'.format(kept,
                                                       len(dropped))
    if dropped:
        causes = sorted({'{} {}'.format(entry['path'] or '(no path)',
                                        entry['problem'])
                         for entry in dropped})
        return 'the model emitted {} block header(s), none usable: {}'.format(
            len(dropped), '; '.join(causes))
    return ('the model produced no PATCH: or FILE: block at all '
            '({} chars of prose)'.format(len(response_text or '')))


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


def generate_patch(system_prompt, target_file_content,
                   target_function, task_description,
                   code_context, llm_call_fn):
    """Generate a modified version of an existing function.

    Instead of producing new files, reads an existing function and
    produces a replacement. For bug patches and incremental
    improvements to existing code.

    :param system_prompt: System prompt with coding conventions.
    :param target_file_content: Full content of the file to patch.
    :param target_function: Name of the function to modify.
    :param task_description: What needs to change.
    :param code_context: Additional source context.
    :param llm_call_fn: Callable ``(system_prompt, user_prompt)``
        returning a string.
    :returns: Dict with ``patched``, ``function_name``,
        ``explanation``, or None on failure.
    """
    user_prompt = (
        f"Task:\n{task_description}\n\n"
        f"Target file contents:\n```\n{target_file_content}"
        f"\n```\n\n"
        f"Function to modify: {target_function}\n\n"
        f"Additional context:\n{code_context}\n\n"
        f"Produce ONLY the modified function. Include the full "
        f"function definition (def line through the end), not "
        f"just the changed lines. Then explain what changed "
        f"and why.\n\n"
        f"Format:\n"
        f"PATCHED_FUNCTION:\n"
        f"```python\n"
        f"def {target_function}(...):\n"
        f"    ...\n"
        f"```\n\n"
        f"CHANGES:\n"
        f"- What changed and why"
    )

    response = llm_call_fn(system_prompt, user_prompt)
    if not response:
        return None

    return _parse_patch_response(response, target_function)


def _parse_patch_response(response_text, function_name):
    """Parse a patch response into patched code and explanation.

    :param response_text: Raw LLM output.
    :param function_name: The function that was patched.
    :returns: Dict with ``patched``, ``function_name``,
        ``explanation``, or None.
    """
    pattern = re.compile(
        r'PATCHED_FUNCTION:\s*\n```\w*\n(.*?)```',
        re.DOTALL,
    )
    match = pattern.search(response_text)
    if not match:
        return None

    patched = match.group(1).strip()

    changes = ''
    if 'CHANGES:' in response_text:
        changes_start = (
            response_text.index('CHANGES:') + len('CHANGES:'))
        changes = response_text[changes_start:].strip()

    return {
        'patched': patched,
        'function_name': function_name,
        'explanation': changes,
    }


_HEADER_RE = re.compile(
    r'^[\s>#*_-]*'
    r'(PATCH|FILE|FUNCTION|DESCRIPTION|IMPLEMENTATION_NOTES'
    r'|ENGINEER_PROMPT)'
    r'[\s*_`]*:[ \t]*(.*?)\s*$')

_FENCE_RE = re.compile(r'^\s*(`{3,}|~{3,})[ \t]*[\w.+#-]*[ \t]*$')

_PATH_HEADERS = ('PATCH', 'FILE')


def _clean_value(raw):
    """Strip markdown decoration from a header's value."""
    return (raw or '').strip().strip('*`_ ').strip()


def _clean_function(raw):
    """The bare ``def`` name a patch anchors on.

    ``extract_function`` searches for ``def <name>(``, so a qualified
    or signature-bearing answer — ``PlanView.get_topline``,
    ``get_topline(self, plan)``, ``def get_topline`` — is reduced to
    the name itself. Unreduced they were dropped outright: the pattern
    this replaced required ``\\w+`` alone, and a dot is not in it, so
    every method on a class produced nothing.

    :param raw: the ``FUNCTION:`` value as written.
    :returns: the identifier, or '' when there isn't one.
    """
    name = _clean_value(raw).split('(')[0].strip().rstrip(':')
    if name.startswith('def '):
        name = name[4:].strip()
    if '.' in name:
        name = name.rsplit('.', 1)[-1].strip()
    return name if name.isidentifier() else ''


def _read_fenced(lines, start, marker):
    """Consume a fenced body, closing only on an equal-or-longer run.

    :param lines: the whole response, split.
    :param start: index of the first body line.
    :param marker: the opening fence run.
    :returns: ``(body, index_after)``; body is None when the fence
        was never closed, which is what a truncated answer looks
        like from here.
    """
    char, need = marker[0], len(marker)
    body = []
    index = start
    while index < len(lines):
        stripped = lines[index].strip()
        if stripped and set(stripped) == {char} and len(
                stripped) >= need:
            return '\n'.join(body), index + 1
        body.append(lines[index])
        index += 1
    return None, index


def _trim_blank_edges(body):
    """Drop leading/trailing blank lines, keeping indentation.

    Deliberately not ``strip()``: that also eats the first line's
    indentation, which silently dedents a class method's ``def`` and
    hands the splice layer code that cannot compile.
    """
    lines = body.split('\n')
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    return '\n'.join(lines)


def _finish_block(pending, body):
    """Turn accumulated headers plus a body into one block.

    ``problem`` is set instead of raising, so a malformed block is
    reported rather than vanishing — the caller counts both.

    :param pending: headers gathered since the last block.
    :param body: the fenced content, None when there was none.
    :returns: the block dict.
    """
    block = {
        'kind': pending.get('kind'),
        'path': pending.get('path') or '',
        'function': pending.get('function') or '',
        'description': pending.get('description') or '',
        'content': _trim_blank_edges(body) if body else '',
        'problem': '',
    }
    if body is None:
        block['problem'] = (
            'was not followed by a closed code block'
            if pending.get('fenced')
            else 'had no code block after it')
    elif not block['path']:
        block['problem'] = 'named no file path'
    elif block['kind'] == 'PATCH' and not block['function']:
        block['problem'] = 'gave no usable FUNCTION: name'
    elif not block['content'].strip():
        block['problem'] = 'had an empty code block'
    return block


def _scan_blocks(text):
    """Walk the response once, pairing block headers with fences.

    Line-oriented rather than one regex over the whole answer for
    two reasons a real answer runs into: header order and decoration
    vary, and a header must never be recognised *inside* a code
    block — a generated file containing the literal ``FILE:`` would
    otherwise open a second, phantom block. Fences are therefore
    consumed whole, headers only read outside them, and a
    ``FUNCTION:`` survives the ``PATCH:`` that may follow it.

    :data:`_HEADER_RE` tolerates the decoration a chat-tuned model
    adds — ``**PATCH: x.py**``, a bullet, a markdown heading, a
    backticked value — every one of which the exact-prefix match this
    replaced dropped silently. Case stays significant: the tokens are
    shouted in the prompt, and matching ``file:`` mid-sentence would
    invent blocks out of prose.

    :param text: raw LLM response.
    :returns: list of block dicts, malformed ones included with a
        ``problem`` set.
    """
    lines = (text or '').splitlines()
    blocks, pending = [], {}
    index = 0
    while index < len(lines):
        fence = _FENCE_RE.match(lines[index])
        if fence:
            body, index = _read_fenced(
                lines, index + 1, fence.group(1))
            if pending.get('kind'):
                pending['fenced'] = True
                blocks.append(_finish_block(pending, body))
                pending = {}
            continue
        header = _HEADER_RE.match(lines[index])
        index += 1
        if not header:
            continue
        key, value = header.group(1), header.group(2)
        if key in _PATH_HEADERS:
            if pending.get('kind'):
                blocks.append(_finish_block(pending, None))
                pending = {}
            pending['kind'] = key
            pending['path'] = _clean_value(value)
        elif key == 'FUNCTION':
            pending['function'] = _clean_function(value)
        elif key == 'DESCRIPTION':
            pending['description'] = _clean_value(value)
        elif pending.get('kind'):
            blocks.append(_finish_block(pending, None))
            pending = {}
    if pending.get('kind'):
        blocks.append(_finish_block(pending, None))
    return blocks


def _extract_section(text, header):
    """Extract a named trailing section from LLM output.

    Shares :data:`_HEADER_RE` with the block scanner so a decorated
    ``**IMPLEMENTATION_NOTES:**`` is found here too, and so the
    section stops at whatever header comes next rather than running
    to the end of the answer.

    :param text: Raw LLM response.
    :param header: Section name, without the colon.
    :returns: Section content string, or empty string.
    """
    collected, capturing = [], False
    for line in (text or '').splitlines():
        match = _HEADER_RE.match(line)
        if match:
            capturing = match.group(1) == header
            if capturing and match.group(2):
                collected.append(match.group(2))
            continue
        if capturing:
            collected.append(line)
    return '\n'.join(collected).strip()

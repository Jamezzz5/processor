"""Ticket intent detection for ALI.

Detects when a user message looks like a feature request, bug report,
or issue — and decides whether to offer ticket creation or guide the
user to an existing feature based on retrieval results.

This module lives in the processor submodule. It must NOT import
anything from app/ or app.features — it works only with data
passed to it.
"""
import re


INTAKE_PATTERNS = [
    re.compile(p, re.IGNORECASE) for p in [
        (r'\b(?:i wish|it would be nice|can we add|could we have'
         r'|we need|we should)\b'),
        (r'\b(?:there\'?s a bug|something\'?s broken'
         r'|it\'?s not working|i found an issue)\b'),
        (r'\b(?:feature request|new feature|improvement'
         r'|enhancement)\b'),
        r'\b(?:can ali|can you add|i want to request)\b',
        (r'\b(?:would it be possible|is there a way'
         r'|any chance we could)\b'),
        (r'\b(?:this doesn\'?t work|not working correctly'
         r'|keeps breaking|wrong data)\b'),
        (r'\b(?:add|build|create|make)\b.{0,40}\b(?:button|feature'
         r'|page|screen|export|toggle|dropdown|integration'
         r'|shortcut|widget|tab)\b'),
        r'\b(?:is|are|seems?\s+to\s+be)\s+broken\b',
    ]
]

CONTENT_TASK_PATTERNS = [
    re.compile(p, re.IGNORECASE) for p in [
        (r'\b(?:emoji|emojis|rewrite|reword|rephrase|paraphrase'
         r'|proofread|spell\s*check|copy\s*edit|punch\s*ier'
         r'|punch\s*it\s*up)\b'),
        r'\b(?:summari[sz]e|tl;?dr)\b',
        r'\b(?:draft|compose)\s+(?:me\s+)?(?:an?|the|some|this)\b',
        r'\b(?:make|turn|format)\s+(?:this|it|the\s+following|these)\b',
        r'\btranslate\b',
    ]
]

GUIDANCE_MODELS = {'TutorialStage', 'Notes', 'WalkthroughSlide'}

DOC_STYLE_PATTERNS = [
    re.compile(p, re.IGNORECASE) for p in [
        r'\bhow\s+(?:do|can|should)\s+(?:i|we|you)\b',
        r'\bhow\s+to\b',
        r'\b(?:where|what)\s+is\s+(?:the\s+)?(?:doc|docs|'
        r'documentation|tutorial|walkthrough|guide)\b',
        r'\b(?:show|find|read)\s+(?:me\s+)?(?:the\s+)?'
        r'(?:doc|docs|documentation|tutorial|walkthrough|guide)',
        r'\b(?:doc|docs|documentation|tutorial|walkthrough|'
        r'guide|instructions)\b',
        r'\bexplain\s+(?:how|what|why)\b',
    ]
]


def is_doc_style_prompt(message):
    """True if ``message`` looks like a documentation-seeking
    query.

    Used to gate the "Could not find matching docs in X"
    fallback message so it only surfaces when the user
    actually asked about docs.
    """
    if not message:
        return False
    return any(p.search(message) for p in DOC_STYLE_PATTERNS)


def command_region(message):
    """Return the leading imperative clause of ``message``.

    Imperative commands lead with their verb; pasted content
    (slide text, lists, quotes) trails after a delimiter. We cut
    at the first newline and at the first ``": "`` -- but only when
    the leading clause is itself a content-authoring lead-in
    ("emojis in this slide: <pasted>"), so a create/edit trigger
    buried in pasted content is not mistaken for a command. A plain
    descriptive preamble ("...through chat or UI: add a partner")
    keeps its trailing command. ``"add a plan"`` stays a command;
    ``"...slide: ... Add ... Plan"`` does not.
    """
    if not message:
        return ''
    head = message.split('\n', 1)[0]
    m = re.search(r':\s', head)
    if m and m.start() >= 3 and is_content_task(head[:m.start()]):
        head = head[:m.start()]
    return head


def is_content_task(message):
    """True if ``message`` is a content-authoring / rewriting
    request (add emojis, draft a slide, summarize this, ...).

    These are conversational LLM tasks — never object mutations
    or ticket intake — even when the pasted content happens to
    contain words like 'add', 'new', or a model name. Used to
    gate both the heuristic create/edit path and the ticket
    offer so a request like "add emojis to this slide" can't
    edit a column or open a ticket.
    """
    if not message:
        return False
    return any(p.search(message) for p in CONTENT_TASK_PATTERNS)


def detect_ticket_intent(message):
    """Check if a message matches ticket/request intake patterns.

    Args:
        message: The user's raw message string.

    Returns:
        True if any intake pattern matches, False otherwise.
    """
    if not message:
        return False
    return any(p.search(message) for p in INTAKE_PATTERNS)


def check_existing_feature_coverage(retrieval_results,
                                    models_searched):
    """Determine if retrieval found relevant existing docs.

    Called after AliChat's normal document retrieval pipeline.

    Args:
        retrieval_results: Response text from the retrieval loop.
        models_searched: List of model classes that produced
            results during the search phase.

    Returns:
        dict with has_existing_docs (bool) and
        guidance_model_names (list of str).
    """
    if not retrieval_results or not retrieval_results.strip():
        return {
            'has_existing_docs': False,
            'guidance_model_names': [],
        }
    matched_guidance = [
        m.__name__ for m in models_searched
        if m.__name__ in GUIDANCE_MODELS
    ]
    return {
        'has_existing_docs': bool(matched_guidance),
        'guidance_model_names': matched_guidance,
    }


def build_ticket_offer_response(has_existing_docs,
                                guidance_model_names,
                                retrieval_response=''):
    """Build ALI's response when ticket intent is detected.

    Two paths:
    1. Existing docs found — guide user first, offer ticket
       as fallback.
    2. No docs found — offer to open a ticket directly.

    Args:
        has_existing_docs: Whether retrieval found docs.
        guidance_model_names: Which doc models matched.
        retrieval_response: Existing retrieval text to include.

    Returns:
        str: ALI's response text.
    """
    if has_existing_docs and retrieval_response.strip():
        return (
            f"{retrieval_response}\n\n"
            "I found some existing documentation that might "
            "address this. Take a look above — if it doesn't "
            "cover what you need, say **open a ticket** and "
            "I'll capture your request properly."
        )
    return (
        "It sounds like you're describing a feature request "
        "or reporting an issue. Want me to open a ticket for "
        "this? I'll ask a few questions to make sure we "
        "capture everything clearly.\n\n"
        "Just say **yes** to start, or keep going and I'll "
        "answer your question."
    )

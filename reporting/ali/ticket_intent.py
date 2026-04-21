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
    ]
]

GUIDANCE_MODELS = {'TutorialStage', 'Notes', 'WalkthroughSlide'}


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

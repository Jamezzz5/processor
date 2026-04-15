"""AliSearch — Combined document retrieval for ALI.

Merges results from three sources:
1. Live keyword index (find_db_model) — always fresh, catches new objects
2. BM25 scoring (TfIdfTransformer) — better relevance ranking
3. TF-IDF cosine similarity (TfIdfTransformer) — semantic similarity

The transformer and text_dict are passed in from the app layer
(loaded from TransformerModel in the database). No globals — the app
server manages caching and passes the transformer to AliSearch.

Usage:
    ali_search = AliSearch(ali_chat, transformer=transformer,
                           transformer_dict=text_dict)
    model_ids = ali_search.search(db_model, message)

Returns {model_id: {'combined': float, 'live': float,
                     'bm25': float, 'tfidf': float}}.
"""
import logging

logger = logging.getLogger(__name__)

# Score weights for combining methods
WEIGHT_LIVE = 1.0
WEIGHT_BM25 = 0.6
WEIGHT_TFIDF = 0.4


def _empty_scores():
    """Return a fresh score dict for a single result."""
    return {
        'combined': 0.0,
        'live': 0.0,
        'bm25': 0.0,
        'tfidf': 0.0,
    }


class AliSearch:
    """Combined retrieval that merges live keyword search with cached
    TF-IDF/BM25 scoring from the existing TfIdfTransformer."""

    def __init__(self, ali_chat, transformer=None,
                 transformer_dict=None):
        """
        :param ali_chat: AliChat instance (provides find_db_model,
            remove_stop_words_from_message,
            get_unigrams_and_bigrams)
        :param transformer: TfIdfTransformer instance (optional,
            loaded from TransformerModel on the app side)
        :param transformer_dict: Dict mapping doc index to
            {model, id, text} metadata
        """
        self.ali_chat = ali_chat
        self.transformer = transformer
        self.transformer_dict = transformer_dict or {}

    def search(self, db_model, message, top_k=5):
        """Combined search returning {model_id: scores} dict.

        Runs live keyword search (always) and transformer search
        (when transformer is provided). Merges and re-ranks.

        :param db_model: SQLAlchemy model class to search
        :param message: User query string
        :param top_k: Max results to return
        :returns: Dict of {model_id: score_dict} where score_dict
            has keys combined, live, bm25, tfidf
        """
        results = {}
        self._live_search(results, db_model, message)
        if (self.transformer is not None
                and self.transformer_dict):
            self._transformer_search(
                results, db_model, message, top_k)
        if len(results) > top_k:
            sorted_items = sorted(
                results.items(),
                key=lambda x: x[1]['combined'],
                reverse=True)
            results = dict(sorted_items[:top_k])
        return results

    def _live_search(self, results, db_model, message):
        """Run the existing find_db_model() keyword search."""
        try:
            model_ids, words = self.ali_chat.find_db_model(
                db_model, message)
            if model_ids:
                max_score = max(model_ids.values())
                for model_id, score in model_ids.items():
                    normalized = (
                        score / max_score * WEIGHT_LIVE
                        if max_score > 0 else 0)
                    entry = results.setdefault(
                        model_id, _empty_scores())
                    entry['live'] = normalized
                    entry['combined'] += normalized
        except Exception as e:
            logger.warning(
                'Live search failed for {}: {}'.format(
                    db_model.__name__, e))

    def _transformer_search(self, results, db_model,
                            message, top_k):
        """Run BM25 + TF-IDF cosine from the transformer."""
        try:
            model_name = db_model.__name__
            bm25_scores = self.transformer.bm25_search(
                message, top_k=top_k * 2)
            tfidf_scores = self.transformer.search(
                message, top_k=top_k * 2)
            _merge_transformer_scores(
                results, bm25_scores,
                self.transformer_dict, model_name,
                weight=WEIGHT_BM25, score_key='bm25')
            _merge_transformer_scores(
                results, tfidf_scores,
                self.transformer_dict, model_name,
                weight=WEIGHT_TFIDF, score_key='tfidf')
        except Exception as e:
            logger.warning(
                'Transformer search failed: {}'.format(e))


def _merge_transformer_scores(results, scores, text_dict,
                              model_name, weight=0.5,
                              score_key='bm25'):
    """Merge transformer search scores into results dict.

    Normalizes scores and filters to the target model type.

    :param results: Dict of {model_id: score_dict} to update
    :param scores: List of (doc_index, score) from transformer
    :param text_dict: Dict mapping index to {model, id, text}
    :param model_name: Only include results matching this model
    :param weight: Weight multiplier for these scores
    :param score_key: Key in score_dict for this algorithm
    """
    if not scores:
        return
    max_score = max(s for _, s in scores) if scores else 1
    if max_score <= 0:
        return
    for doc_idx, score in scores:
        idx_str = str(doc_idx)
        if idx_str not in text_dict:
            continue
        doc_info = text_dict[idx_str]
        if doc_info.get('model') != model_name:
            continue
        model_id = doc_info.get(
            '{}_id'.format(model_name.lower()),
            doc_info.get('id'))
        if model_id is None:
            continue
        normalized = (score / max_score) * weight
        entry = results.setdefault(
            model_id, _empty_scores())
        entry[score_key] = normalized
        entry['combined'] += normalized

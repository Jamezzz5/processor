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
import time

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
                 transformer_dict=None, stats=None):
        """
        :param ali_chat: AliChat instance (provides find_db_model,
            remove_stop_words_from_message,
            get_unigrams_and_bigrams)
        :param transformer: TfIdfTransformer instance (optional,
            loaded from TransformerModel on the app side)
        :param transformer_dict: Dict mapping doc index to
            {model, id, text} metadata
        :param stats: Optional dict the search accumulates timing and
            hit counters into, so the caller can persist where a
            retrieval pass actually spent its time. ``None`` (the
            default) disables accounting entirely — the counters are
            for the app's telemetry, not for search itself.
        """
        self.ali_chat = ali_chat
        self.transformer = transformer
        self.transformer_dict = transformer_dict or {}
        self.stats = stats

    def _bump(self, key, value):
        """Add ``value`` to a counter in the optional stats sink.

        :param key: counter name.
        :param value: amount to add.
        :returns: None
        """
        if self.stats is None:
            return
        self.stats[key] = round(self.stats.get(key, 0) + value, 2)

    def search(self, db_model, message, top_k=5, components=None):
        """Combined search returning {model_id: scores} dict.

        Runs live keyword search and transformer search (when the
        transformer is provided). Merges and re-ranks.

        :param db_model: SQLAlchemy model class to search
        :param message: User query string
        :param top_k: Max results to return
        :param components: Optional iterable subset of
            ``{'live', 'bm25', 'tfidf'}`` restricting which score
            sources run. ``None`` (default) runs everything — the
            app layer's runtime toggles pass an explicit subset.
        :returns: Dict of {model_id: score_dict} where score_dict
            has keys combined, live, bm25, tfidf
        """
        enabled = (set(components) if components is not None
                   else {'live', 'bm25', 'tfidf'})
        results = {}
        self._bump('models_searched', 1)
        if 'live' in enabled:
            t0 = time.perf_counter()
            self._live_search(results, db_model, message)
            self._bump('live_ms',
                       (time.perf_counter() - t0) * 1000)
            self._bump('live_hits', len(results))
        if (self.transformer is not None
                and self.transformer_dict
                and enabled & {'bm25', 'tfidf'}):
            before = len(results)
            t0 = time.perf_counter()
            self._transformer_search(
                results, db_model, message, top_k,
                enabled=enabled)
            self._bump('transformer_ms',
                       (time.perf_counter() - t0) * 1000)
            self._bump('transformer_hits', len(results) - before)
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
                            message, top_k, enabled=None):
        """Run BM25 + TF-IDF cosine from the transformer.

        Each model gets its OWN top slice of the corpus scores.
        Slicing the global top-k first and filtering by model
        after (the old order) starved every model that doesn't
        dominate the corpus — Notes is a large share of the
        trained docs, so other models' bm25/tfidf almost never
        survived the filter and the scores went unused.

        :param enabled: Optional set restricting which of the two
            transformer scores merge (``'bm25'`` / ``'tfidf'``).
        """
        try:
            enabled = enabled or {'bm25', 'tfidf'}
            model_name = db_model.__name__
            by_model = self._scores_by_model(message)
            if 'bm25' in enabled:
                _merge_transformer_scores(
                    results,
                    by_model['bm25'].get(model_name, [])[:top_k * 2],
                    self.transformer_dict, model_name,
                    weight=WEIGHT_BM25, score_key='bm25')
            if 'tfidf' in enabled:
                _merge_transformer_scores(
                    results,
                    by_model['tfidf'].get(model_name, [])[:top_k * 2],
                    self.transformer_dict, model_name,
                    weight=WEIGHT_TFIDF, score_key='tfidf')
        except Exception as e:
            logger.warning(
                'Transformer search failed: {}'.format(e))

    def _full_scores(self, message):
        """Full sorted BM25 + TF-IDF score lists for ``message``.

        The scores don't depend on the model being searched, but
        get_response searches every chat model with the same
        message — so both corpus scans are memoized on the per-
        request ``ali_chat`` and computed once per message instead
        of once per model.
        """
        cache = getattr(self.ali_chat, 'transformer_score_cache', None)
        if cache is None:
            cache = {}
            setattr(self.ali_chat, 'transformer_score_cache', cache)
        if message not in cache:
            n_docs = len(self.transformer_dict)
            self._bump('corpus_docs', n_docs)
            t0 = time.perf_counter()
            bm25 = self.transformer.bm25_search(message, top_k=n_docs)
            t1 = time.perf_counter()
            tfidf = self.transformer.search(message, top_k=n_docs)
            t2 = time.perf_counter()
            self._bump('bm25_scan_ms', (t1 - t0) * 1000)
            self._bump('tfidf_scan_ms', (t2 - t1) * 1000)
            cache[message] = (bm25, tfidf)
        return cache[message]

    def _scores_by_model(self, message):
        """``{'bm25'|'tfidf': {model_name: [(idx, score), ...]}}``.

        Groups the full sorted score lists by the doc's model,
        keeping per-model rank order and dropping non-positive
        scores — the full lists include every corpus doc, and
        zero-score docs would otherwise surface as junk matches
        on messages with no term overlap. Memoized per message
        alongside the full lists.
        """
        bm25_full, tfidf_full = self._full_scores(message)
        cache = getattr(self.ali_chat, 'transformer_score_cache')
        cache_key = (message, 'by_model')
        if cache_key not in cache:
            group_start = time.perf_counter()
            grouped = {'bm25': {}, 'tfidf': {}}
            for key, full in (('bm25', bm25_full),
                              ('tfidf', tfidf_full)):
                for doc_idx, score in full:
                    if score <= 0:
                        continue
                    doc_info = self.transformer_dict.get(str(doc_idx))
                    if not doc_info:
                        continue
                    model_name = doc_info.get('model')
                    grouped[key].setdefault(
                        model_name, []).append((doc_idx, score))
            self._bump('group_ms',
                       (time.perf_counter() - group_start) * 1000)
            cache[cache_key] = grouped
        return cache[cache_key]


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

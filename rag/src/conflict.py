import numpy as np
import scipy.special
from dataclasses import dataclass
from sentence_transformers import CrossEncoder

from src.preprocessor import RankedChunk
from src.settings import Settings


@dataclass
class ConflictPair:
    """Represents a detected conflict between two knowledge chunks."""
    chunk_a: RankedChunk
    chunk_b: RankedChunk
    confidence: float


class ConflictDetector:
    """Detects semantic contradictions between retrieved chunks using NLI."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.model = CrossEncoder(self.settings.nli_model)

    def detect_conflicts(self, chunks: list[RankedChunk]) -> list[ConflictPair]:
        """
        Run pairwise Natural Language Inference (NLI) on the provided chunks.
        
        Parameters:
            chunks: The top-ranked chunks to cross-check.
            
        Returns:
            A list of ConflictPair objects for chunks that contradict each other.
        """
        conflict_pairs: list[ConflictPair] = []
        if len(chunks) < 2:
            return conflict_pairs

        # Find the index for CONTRADICTION in the model's labels
        id2label = getattr(self.model.config, "id2label", {})
        contradiction_idx = -1
        for idx, label in id2label.items():
            if label.upper() == "CONTRADICTION":
                contradiction_idx = idx
                break
                
        if contradiction_idx == -1:
            # Fallback assumption for cross-encoder/nli-deberta-v3-small
            contradiction_idx = 0

        # Build pairs
        pairs = []
        pair_indices = []
        for i in range(len(chunks)):
            for j in range(i + 1, len(chunks)):
                pairs.append((chunks[i].content, chunks[j].content))
                pair_indices.append((i, j))

        if not pairs:
            return []

        # Predict logits
        scores = self.model.predict(pairs)
        
        # In case it returns a list of arrays, make sure it's a 2D numpy array
        scores = np.array(scores)
        
        # Apply softmax to get probabilities
        probs = scipy.special.softmax(scores, axis=1)

        # Check thresholds
        for idx, (i, j) in enumerate(pair_indices):
            pair_probs = probs[idx]
            pred_class = np.argmax(pair_probs)
            confidence = float(pair_probs[contradiction_idx])
            
            if pred_class == contradiction_idx and confidence > self.settings.conflict_confidence_threshold:
                conflict_pairs.append(ConflictPair(
                    chunk_a=chunks[i],
                    chunk_b=chunks[j],
                    confidence=confidence
                ))

        return conflict_pairs

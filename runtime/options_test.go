package runtime

// Test-only knobs: the production API exposes neither (the values are fixed), but tests shrink the
// Tier-1 byte budget and the promotion threshold to reach eviction and promotion quickly.

func withMaxPartialBytes(bytes int) CacheOption {
	return func(cfg *cacheConfig) {
		cfg.MaxPartialBytes = bytes
	}
}

func withPromoteThreshold(threshold uint8) CacheOption {
	return func(cfg *cacheConfig) {
		cfg.PromoteThreshold = min(max(threshold, 1), 15)
	}
}

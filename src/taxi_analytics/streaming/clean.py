from taxi_analytics.pipeline.clean import clean_trips

# Re-export for convenience — streaming uses the same clean logic
__all__ = ["clean_trips"]

twitter-sentiment-tracker
=========================

A very simple sentiment tracker using Storm and the twitter streaming API. Since the streaming API only allows limited filtering (no intersection), Storm is used for detailled filtering. Results are persisted to MongoDB and recovered on startup.
